// demo-agent exercises streaming and durable files without any LLM credential.
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	dir := os.Getenv("WORKSPACE_DIR")
	if dir == "" {
		dir = "/workspace"
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		log.Fatal(err)
	}
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "workspace=%s\nGET /events · GET/PUT /note\n", os.Getenv("WORKSPACE_ID"))
	})
	mux.HandleFunc("PUT /note", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
		if err != nil {
			http.Error(w, "note too large", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		err = os.WriteFile(filepath.Join(dir, "note.tmp"), b, 0600)
		if err == nil {
			err = os.Rename(filepath.Join(dir, "note.tmp"), filepath.Join(dir, "note.txt"))
		}
		if err != nil {
			http.Error(w, "cannot persist note", 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /note", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		b, err := os.ReadFile(filepath.Join(dir, "note.txt"))
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, "cannot read note", 500)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := http.NewResponseController(w)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for i := 0; i < 60; i++ {
			if _, err := fmt.Fprintf(w, "data: %d\n\n", i); err != nil {
				return
			}
			if err := flusher.Flush(); err != nil {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
