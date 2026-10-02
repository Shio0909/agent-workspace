package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
)

func toolSpecs() []map[string]any {
	fn := func(name, desc string, props map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "function", "function": map[string]any{
			"name": name, "description": desc,
			"parameters": map[string]any{"type": "object", "properties": props, "required": required},
		}}
	}
	str := map[string]any{"type": "string"}
	return []map[string]any{
		fn("write_file", "Write a text file in the workspace.", map[string]any{"path": str, "content": str}, "path", "content"),
		fn("read_file", "Read a text file from the workspace.", map[string]any{"path": str}, "path"),
		fn("list_files", "List files in the workspace.", map[string]any{}),
	}
}

// runTool never returns an error to the caller: a failed tool is reported back
// to the model as text, which is how the model learns to correct itself.
func (a *Agent) runTool(call toolCall) string {
	out, err := a.dispatch(call.Function.Name, call.Function.Arguments)
	if err != nil {
		out = "error: " + err.Error()
	}
	if len(out) > maxToolOutput {
		out = out[:maxToolOutput] + "\n[truncated]"
	}
	return out
}

// files opens the confined area the tools may touch. os.Root rejects "..",
// absolute paths and symlinks that point outside, so the model cannot reach
// sessions or anything else on the volume.
func (a *Agent) files() (*os.Root, error) {
	dir := a.cfg.WorkspaceDir + "/files"
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return os.OpenRoot(dir)
}

func (a *Agent) dispatch(name, rawArgs string) (string, error) {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if rawArgs != "" {
		if err := json.Unmarshal([]byte(rawArgs), &args); err != nil {
			return "", errors.New("arguments are not valid JSON")
		}
	}
	root, err := a.files()
	if err != nil {
		return "", err
	}
	defer root.Close()
	switch name {
	case "write_file":
		p, err := cleanPath(args.Path)
		if err != nil {
			return "", err
		}
		if len(args.Content) > maxFileBytes {
			return "", fmt.Errorf("file exceeds %d bytes", maxFileBytes)
		}
		if dir := path.Dir(p); dir != "." {
			if err := root.MkdirAll(dir, 0o750); err != nil {
				return "", err
			}
		}
		if err := root.WriteFile(p, []byte(args.Content), 0o640); err != nil {
			return "", err
		}
		return fmt.Sprintf("wrote %d bytes to %s", len(args.Content), p), nil
	case "read_file":
		p, err := cleanPath(args.Path)
		if err != nil {
			return "", err
		}
		b, err := root.ReadFile(p)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case "list_files":
		var names []string
		err := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				names = append(names, p)
			}
			return err
		})
		if err != nil {
			return "", err
		}
		if len(names) == 0 {
			return "(no files)", nil
		}
		return strings.Join(names, "\n"), nil
	}
	return "", fmt.Errorf("unknown tool %q", name)
}

func cleanPath(p string) (string, error) {
	if p == "" || strings.HasPrefix(p, "/") {
		return "", errors.New("path must be relative")
	}
	c := path.Clean(p)
	if c == "." || c == ".." || strings.HasPrefix(c, "../") {
		return "", errors.New("path escapes the workspace")
	}
	return c, nil
}
