// agent-runtime is the workload the controller hosts in a workspace: a ReAct
// agent that reads its LLM key from a mounted credential directory.
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"agent-workspace/internal/agent"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	steps, _ := strconv.Atoi(env("AGENT_MAX_STEPS", "6"))
	a := agent.New(agent.Config{
		WorkspaceID:   os.Getenv("WORKSPACE_ID"),
		WorkspaceDir:  env("WORKSPACE_DIR", "/workspace"),
		CredentialDir: env("CREDENTIAL_DIR", "/var/run/agent-credentials"),
		LLMBaseURL:    env("LLM_BASE_URL", "http://fake-llm:8081/v1"),
		Model:         env("LLM_MODEL", "default"),
		MaxSteps:      steps,
	})
	server := &http.Server{Addr: ":8080", Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
