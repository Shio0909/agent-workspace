package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"

	"agent-workspace/internal/control"
)

// ActorPrefix 标记审计里由令牌决定的归属。作用域令牌的归属一律由令牌名生成，
// 忽略 X-Actor；共享令牌自报的 X-Actor 带这个前缀时不被采纳，所以审计里
// 带这个前缀的记录确实来自对应的令牌。
const ActorPrefix = "token:"

var tokenNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ScopedToken 让持有者只能操作一部分工作区。服务端只保存令牌的 SHA-256：
// 配置文件泄露不会泄露可用的令牌，而一个 256 位随机令牌的摘要无法反推。
type ScopedToken struct {
	Name   string
	Digest [sha256.Size]byte
	// Workspaces 是精确的工作区 ID，或"前缀*"。为空表示不按 ID 限制。
	Workspaces []string
	// Profiles 为空表示不按 profile 限制。两个限制同时给出时取交集。
	Profiles []string
}

func (t *ScopedToken) allowsID(id string) bool {
	if len(t.Workspaces) == 0 {
		return true
	}
	for _, pattern := range t.Workspaces {
		if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
			if strings.HasPrefix(id, prefix) {
				return true
			}
		} else if pattern == id {
			return true
		}
	}
	return false
}

func (t *ScopedToken) allowsProfile(profile string) bool {
	return len(t.Profiles) == 0 || slices.Contains(t.Profiles, profile)
}

// principal 是通过认证的调用方。token 为 nil 表示共享的管理员令牌。
type principal struct{ token *ScopedToken }

func (p *principal) admin() bool { return p != nil && p.token == nil }

type principalKey struct{}

func principalOf(r *http.Request) *principal {
	p, _ := r.Context().Value(principalKey{}).(*principal)
	return p
}

// authenticate 先比共享令牌，再比作用域令牌的摘要。两处都是常量时间比较，
// 且摘要比较不在命中后提前退出，耗时不随命中位置变化。
func (s *Server) authenticate(r *http.Request) (*principal, bool) {
	presented := r.Header.Get("X-Control-Token")
	if s.Token != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(s.Token)) == 1 {
		return &principal{}, true
	}
	if presented == "" || len(s.Tokens) == 0 {
		return nil, false
	}
	sum := sha256.Sum256([]byte(presented))
	var match *ScopedToken
	for i := range s.Tokens {
		if subtle.ConstantTimeCompare(sum[:], s.Tokens[i].Digest[:]) == 1 {
			match = &s.Tokens[i]
		}
	}
	if match == nil {
		return nil, false
	}
	return &principal{token: match}, true
}

// mayUse 判断调用方能否操作某个工作区。工作区不存在时，只按 ID 限制的令牌
// 放行（由处理函数自己返回 404），带 profile 限制的令牌拒绝，因为无从判断。
// 没有通过认证的请求一律拒绝，所以漏掉认证的路由不会变成开放路由。
func (s *Server) mayUse(r *http.Request, id string) bool {
	p := principalOf(r)
	switch {
	case p == nil:
		return false
	case p.token == nil:
		return true
	case !p.token.allowsID(id):
		return false
	case len(p.token.Profiles) == 0:
		return true
	}
	w, err := s.Controller.Get(id)
	return err == nil && p.token.allowsProfile(w.Profile)
}

// forWorkspace 限制带 {id} 的路由。越权返回的就是"不存在"的 404，响应与真的
// 不存在的工作区一致，作用域令牌无法借此探测别的租户有哪些工作区。
func (s *Server) forWorkspace(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.mayUse(r, r.PathValue("id")) {
			fail(w, control.ErrNotFound)
			return
		}
		next(w, r)
	}
}

func forbidden(w http.ResponseWriter) {
	respond(w, http.StatusForbidden, map[string]string{"error": "forbidden for this token"})
}

// adminOnly 是没有 {id} 的路由的默认限制，例如 /metrics：它暴露所有工作区的
// 数量。需要自己做作用域判断的路由必须显式用 open 注册。
func (s *Server) adminOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !principalOf(r).admin() {
			forbidden(w)
			return
		}
		next(w, r)
	}
}

// router 让"忘了加限制"变成拒绝而不是放行：带 {id} 的路由按工作区限制，其余
// 路由默认只给管理员。
type router struct {
	*http.ServeMux
	s *Server
}

func (rt router) HandleFunc(pattern string, h func(http.ResponseWriter, *http.Request)) {
	if strings.Contains(pattern, "{id}") {
		h = rt.s.forWorkspace(h)
	} else {
		h = rt.s.adminOnly(h)
	}
	rt.ServeMux.HandleFunc(pattern, h)
}

// open 注册不带默认限制的路由：探针，以及自己按作用域过滤或校验的路由。
func (rt router) open(pattern string, h func(http.ResponseWriter, *http.Request)) {
	rt.ServeMux.HandleFunc(pattern, h)
}

type tokenEntry struct {
	Name       string   `json:"name"`
	SHA256     string   `json:"sha256"`
	Workspaces []string `json:"workspaces"`
	Profiles   []string `json:"profiles"`
}

// LoadScopedTokens 读取作用域令牌文件。和 profile 文件一样拒绝未知字段：
// 一个拼错的 "workspace" 若被静默忽略，令牌就会比本意宽得多。
func LoadScopedTokens(path string, profiles map[string]control.Profile) ([]ScopedToken, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var entries []tokenEntry
	if err := dec.Decode(&entries); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("parse %s: unexpected data after the token list", path)
	}
	if len(entries) == 0 {
		return nil, errors.New("no scoped tokens configured")
	}
	names := map[string]bool{}
	digests := map[[sha256.Size]byte]string{}
	out := make([]ScopedToken, 0, len(entries))
	for i, e := range entries {
		if !tokenNamePattern.MatchString(e.Name) {
			return nil, fmt.Errorf("token %d: invalid name %q", i, e.Name)
		}
		if names[e.Name] {
			return nil, fmt.Errorf("token %s: duplicate name", e.Name)
		}
		names[e.Name] = true
		raw, err := hex.DecodeString(e.SHA256)
		if err != nil || len(raw) != sha256.Size {
			return nil, fmt.Errorf("token %s: sha256 must be %d hex characters", e.Name, 2*sha256.Size)
		}
		var digest [sha256.Size]byte
		copy(digest[:], raw)
		if other, dup := digests[digest]; dup {
			return nil, fmt.Errorf("token %s: same token as %s", e.Name, other)
		}
		digests[digest] = e.Name
		// 没有任何限制的条目等于另一个管理员令牌，但管理员令牌只有一个。
		if len(e.Workspaces) == 0 && len(e.Profiles) == 0 {
			return nil, fmt.Errorf("token %s: needs workspaces or profiles; the shared token is the only unrestricted one", e.Name)
		}
		for _, pattern := range e.Workspaces {
			// 前缀为空（单独的 "*"）会匹配所有工作区，等价于没有限制。
			if !control.ValidName(strings.TrimSuffix(pattern, "*")) {
				return nil, fmt.Errorf("token %s: workspace %q must be an id or a non-empty prefix followed by *", e.Name, pattern)
			}
		}
		for _, name := range e.Profiles {
			if _, ok := profiles[name]; !ok {
				return nil, fmt.Errorf("token %s: unknown profile %q", e.Name, name)
			}
		}
		out = append(out, ScopedToken{Name: e.Name, Digest: digest, Workspaces: e.Workspaces, Profiles: e.Profiles})
	}
	return out, nil
}

// TokenDigest 是令牌文件里 sha256 字段的计算方式。
func TokenDigest(token string) [sha256.Size]byte { return sha256.Sum256([]byte(token)) }

// ShadowsToken 报告某个作用域令牌是否和共享令牌相同。这样的条目永远不会
// 生效（共享令牌先匹配），留着只会让人误以为它被限制住了。
func ShadowsToken(tokens []ScopedToken, shared string) (string, bool) {
	digest := TokenDigest(shared)
	for _, t := range tokens {
		if t.Digest == digest {
			return t.Name, true
		}
	}
	return "", false
}

func withPrincipal(r *http.Request, p *principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalKey{}, p))
}
