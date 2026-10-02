// fake-llm serves a deterministic OpenAI-compatible endpoint for the demo and
// rotation tests. It is not a model.
package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"agent-workspace/internal/agent/fakellm"
)

func main() {
	s := fakellm.New(strings.Split(os.Getenv("FAKE_LLM_KEYS"), ",")...)
	server := &http.Server{Addr: ":8081", Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
