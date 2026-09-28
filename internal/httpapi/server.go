package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"time"

	"agent-workspace/internal/control"
)

// readinessTimeout 是就绪探针在 HTTP 层的上限，比控制器里的单次探测上限更宽松，
// 留出排队时间，但不会让探针挂住整个 kubelet 周期。
const readinessTimeout = 5 * time.Second

// maxAuditLimit 限制一次审计查询的返回条数。审计文件只追加，没有它一个请求就
// 能把整个文件读进内存。
const maxAuditLimit = 1000

// maxActorLength 截断调用方自报的归属名，避免单条请求把审计日志撑大。
const maxActorLength = 128

// upstreamTransport is shared by every proxied request. http.DefaultTransport
// keeps only two idle connections per host, and each workspace is one host, so
// under concurrency most requests would open a fresh TCP connection and leave
// one in TIME_WAIT when the pool overflows.
var upstreamTransport http.RoundTripper = newUpstreamTransport()

// maxConnsPerWorkspace caps upstream connections to one agent runtime. The idle
// pool is exactly as large, so a returned connection is never closed while the
// cap is in use; requests beyond the cap wait for a connection instead of
// dialing, which is also backpressure for a single agent pod. Long-lived SSE or
// WebSocket streams hold a connection each, so the cap bounds open streams per
// workspace too.
const maxConnsPerWorkspace = 512

func newUpstreamTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 4096
	t.MaxIdleConnsPerHost = maxConnsPerWorkspace
	t.MaxConnsPerHost = maxConnsPerWorkspace
	t.IdleConnTimeout = 90 * time.Second
	return t
}

type Server struct {
	Controller  *control.Controller
	Token       string
	WakeTimeout time.Duration
	Transport   http.RoundTripper
	// Actor 是审计事件的默认归属，可被请求头 X-Actor 覆盖。它只是归属信息：
	// 控制面只校验一个共享令牌，调用方可以声称任意身份。
	Actor string
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// 存活与就绪分开：进程还在就该返回 200；依赖不可用只把实例摘出流量，
	// 而不是让编排系统重启一个其实健康的进程。
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()
		if err := s.Controller.Ready(ctx); err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "compute backend unavailable"})
			return
		}
		respond(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		// 响应头已经写出，这里只能记录失败，不能改状态码。
		if err := s.Controller.WriteMetrics(w); err != nil {
			slog.Error("write metrics", "error", err)
		}
	})
	mux.HandleFunc("GET /v1/audit", func(w http.ResponseWriter, r *http.Request) {
		query, err := auditQuery(r)
		if err != nil {
			fail(w, err)
			return
		}
		events, err := s.Controller.Audit(query)
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusOK, events)
	})
	mux.HandleFunc("GET /v1/workspaces", func(w http.ResponseWriter, r *http.Request) {
		items := s.Controller.List()
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		respond(w, http.StatusOK, items)
	})
	mux.HandleFunc("POST /v1/workspaces", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID      string `json:"id"`
			Profile string `json:"profile"`
		}
		if !decode(w, r, &in) {
			return
		}
		// 创建不需要幂等键：同 ID 同 profile 的重复创建本来就返回已有工作区。
		item, err := s.Controller.Create(s.actor(r), in.ID, in.Profile)
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusCreated, item)
	})
	mux.HandleFunc("GET /v1/workspaces/{id}", func(w http.ResponseWriter, r *http.Request) {
		item, err := s.Controller.Get(r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusOK, item)
	})
	for action, desired := range map[string]string{
		"start":   control.DesiredRunning,
		"stop":    control.DesiredStopped,
		"restart": "",
	} {
		mux.HandleFunc("POST /v1/workspaces/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			s.lifecycle(w, r, action, func(actor, id string) (control.Workspace, error) {
				if action == "restart" {
					return s.Controller.Restart(actor, id)
				}
				return s.Controller.SetDesired(actor, id, desired)
			})
		})
	}
	mux.HandleFunc("DELETE /v1/workspaces/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.lifecycle(w, r, control.OpDelete, func(actor, id string) (control.Workspace, error) {
			return s.Controller.SetDesired(actor, id, control.DesiredDeleted)
		})
	})
	// 续期不要求幂等键：它只写一个绝对时间，重复提交的结果完全相同。
	mux.HandleFunc("POST /v1/workspaces/{id}/expiry", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ExpiresInSeconds int64 `json:"expires_in_seconds"`
		}
		if !decode(w, r, &in) {
			return
		}
		if in.ExpiresInSeconds < 0 || in.ExpiresInSeconds > int64((365*24*time.Hour)/time.Second) {
			fail(w, control.ErrInvalid)
			return
		}
		deadline := time.Time{}
		if in.ExpiresInSeconds > 0 {
			deadline = time.Now().Add(time.Duration(in.ExpiresInSeconds) * time.Second)
		}
		item, err := s.Controller.SetExpiry(s.actor(r), r.PathValue("id"), deadline)
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusOK, item)
	})
	mux.HandleFunc("POST /v1/workspaces/{id}/leases", s.lease)
	mux.HandleFunc("PUT /v1/workspaces/{id}/leases/{lease}", s.lease)
	mux.HandleFunc("DELETE /v1/workspaces/{id}/leases/{lease}", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Controller.ReleaseLease(s.actor(r), r.PathValue("id"), r.PathValue("lease")); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/w/{id}/{path...}", s.proxy)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 健康、就绪和指标之外的接口都要求控制令牌。探针请求来自编排系统，
		// 拿不到业务凭据，所以只有这两个端点放开。
		if requiresToken(r.URL.Path) && (s.Token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Control-Token")), []byte(s.Token)) != 1) {
			respond(w, http.StatusUnauthorized, map[string]string{"error": "invalid control token"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func requiresToken(path string) bool {
	return path != "/health" && path != "/ready"
}

// lifecycle 是所有生命周期写操作的统一入口：先取幂等键并占用记录，只有首次
// 提交才会落到控制器上。重复提交拿到的是第一次的结果，不会产生第二次副作用。
func (s *Server) lifecycle(w http.ResponseWriter, r *http.Request, action string, apply func(actor, id string) (control.Workspace, error)) {
	bizID, ok := s.bizID(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	op, err := s.Controller.BeginOperation(bizID, id, action)
	if replay(w, op, err) {
		return
	}
	item, applyErr := apply(s.actor(r), id)
	if err := s.Controller.FinishOperation(bizID, applyErr); err != nil {
		slog.Error("finish operation", "biz_id", bizID, "workspace", id, "error", err)
	}
	if applyErr != nil {
		fail(w, applyErr)
		return
	}
	respond(w, http.StatusAccepted, item)
}

// bizID 从请求头或 JSON 体里取调用方提供的幂等键。写操作没有它就无法安全
// 重试，所以缺失直接 400，而不是退化成"每次都会执行"。
func (s *Server) bizID(w http.ResponseWriter, r *http.Request) (string, bool) {
	if id := r.Header.Get("X-Biz-Id"); id != "" {
		return id, true
	}
	var in struct {
		BizID string `json:"biz_id"`
	}
	if !decodeOptional(w, r, &in) {
		return "", false
	}
	if in.BizID == "" {
		fail(w, control.ErrInvalid)
		return "", false
	}
	return in.BizID, true
}

// replay 处理重复提交。已完成和进行中都不是错误，所以不走 fail：它们要带着
// 第一次的记录返回。返回 true 表示响应已经写完。
func replay(w http.ResponseWriter, op control.Operation, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, control.ErrOperationInProgress):
		respond(w, http.StatusAccepted, op)
	case errors.Is(err, control.ErrOperationSucceeded):
		respond(w, http.StatusOK, op)
	default:
		fail(w, err)
	}
	return true
}

func (s *Server) actor(r *http.Request) string {
	actor := r.Header.Get("X-Actor")
	if actor == "" {
		actor = s.Actor
	}
	if actor == "" {
		return control.ActorUnknown
	}
	if len(actor) > maxActorLength {
		return actor[:maxActorLength]
	}
	return actor
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TTL int `json:"ttl_seconds"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.TTL < 1 || in.TTL > 3600 {
		fail(w, control.ErrInvalid)
		return
	}
	token, err := s.Controller.Lease(s.actor(r), r.PathValue("id"), r.PathValue("lease"), time.Duration(in.TTL)*time.Second)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, http.StatusOK, map[string]string{"lease": token})
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	timeout := s.WakeTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	target, release, err := s.Controller.Acquire(ctx, r.PathValue("id"))
	cancel() // Only the wake phase is bounded; the response can be a long SSE/WS stream.
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		fail(w, errors.New("invalid runtime endpoint"))
		return
	}
	transport := s.Transport
	if transport == nil {
		transport = upstreamTransport
	}
	proxy := &httputil.ReverseProxy{
		FlushInterval: -1,
		Transport:     transport,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL.Path = "/" + r.PathValue("path")
			p.Out.URL.RawPath = ""
			p.SetURL(u)
			p.Out.Header.Del("X-Control-Token")
			p.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.Controller.InvalidateEndpoint(r.PathValue("id"), target)
			slog.Warn("runtime proxy", "workspace", r.PathValue("id"), "error", err)
			respond(w, http.StatusBadGateway, map[string]string{"error": "runtime unavailable"})
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusServiceUnavailable {
				s.Controller.InvalidateEndpoint(r.PathValue("id"), target)
			}
			return nil
		},
	}
	proxy.ServeHTTP(w, r)
}

func auditQuery(r *http.Request) (control.AuditQuery, error) {
	values := r.URL.Query()
	query := control.AuditQuery{Workspace: values.Get("workspace"), Action: values.Get("action")}
	var err error
	if query.Since, err = parseTime(values.Get("since")); err != nil {
		return query, err
	}
	if query.Until, err = parseTime(values.Get("until")); err != nil {
		return query, err
	}
	if raw := values.Get("limit"); raw != "" {
		query.Limit, err = strconv.Atoi(raw)
		if err != nil || query.Limit < 0 || query.Limit > maxAuditLimit {
			return query, fmt.Errorf("%w: limit must be in [0, %d]", control.ErrInvalid, maxAuditLimit)
		}
	}
	return query, nil
}

func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: since and until must be RFC3339", control.ErrInvalid)
	}
	return at, nil
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeBody(w, r, dst, false)
}

// decodeOptional 允许空请求体：幂等键可以放在请求头里，删除这类请求经常没有体。
func decodeOptional(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeBody(w, r, dst, true)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any, allowEmpty bool) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return true
		}
		fail(w, control.ErrInvalid)
		return false
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		fail(w, control.ErrInvalid)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, control.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, control.ErrConflict), errors.Is(err, control.ErrOperationFailed):
		status = http.StatusConflict
	case errors.Is(err, control.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, control.ErrExpired):
		status = http.StatusGone
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	}
	message := err.Error()
	if status == http.StatusInternalServerError {
		slog.Error("request failed", "error", err)
		message = "internal error; inspect controller logs"
	}
	respond(w, status, map[string]string{"error": message})
}
