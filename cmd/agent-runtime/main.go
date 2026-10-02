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
	retries, _ := strconv.Atoi(env("LLM_RETRIES", "2"))
	timeout, err := time.ParseDuration(env("LLM_TIMEOUT", "180s"))
	if err != nil {
		log.Fatalf("LLM_TIMEOUT: %v", err)
	}
	// Compaction is off until the model's context window is given.
	window, _ := strconv.Atoi(env("AGENT_CONTEXT_TOKENS", "0"))
	reserve, _ := strconv.Atoi(env("AGENT_RESERVE_TOKENS", "0"))
	keep, _ := strconv.Atoi(env("AGENT_KEEP_RECENT_TOKENS", "0"))
	a := agent.New(agent.Config{
		WorkspaceID:   os.Getenv("WORKSPACE_ID"),
		Version:       os.Getenv("AGENT_VERSION"),
		WorkspaceDir:  env("WORKSPACE_DIR", "/workspace"),
		CredentialDir: env("CREDENTIAL_DIR", "/var/run/agent-credentials"),
		LLMBaseURL:    env("LLM_BASE_URL", "http://fake-llm:8081/v1"),
		Model:         env("LLM_MODEL", "default"),
		MaxSteps:      steps,
		LLMTimeout:    timeout,
		LLMRetries:    retries,

		ContextTokens:    window,
		ReserveTokens:    reserve,
		KeepRecentTokens: keep,
	})
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
