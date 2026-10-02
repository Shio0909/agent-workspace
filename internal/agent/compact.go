package agent

// Context compaction for the workspace agent.
//
// A session is replayed to the model on every turn, so without a bound it
// outgrows the context window after enough turns and every later chat fails.
// When the history no longer fits, older turns are replaced by one summary and
// the most recent turns stay verbatim.
//
// This is a deliberately small version of the idea. It compacts only at a turn
// boundary: stored sessions hold complete turns, and one turn is bounded by
// MaxSteps and maxToolOutput, so a turn cannot overflow the window on its own.
// That removes the hard parts of a general compactor (splitting a turn in
// half, repairing a provider's "context too long" error mid-loop). What stays:
//
//   - the cut is chosen by token budget and always falls on a user message, so
//     a tool result is never separated from the call that requested it;
//   - the summary is updated in place instead of summarising a summary;
//   - a summary that was cut off by the token cap, or could not be produced,
//     is never trusted: the older turns fall back to a raw archive.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	defaultReserveTokens = 1024
	minSummaryTokens     = 256

	summaryPrefix = "The conversation before this point was compacted into the following summary:\n\n<summary>\n"
	summarySuffix = "\n</summary>"

	// The wording "summarization assistant" is what the fake LLM keys on.
	summarizerSystem = "You are a context summarization assistant. Read the conversation " +
		"transcript and write the summary in the requested format. Do NOT continue the " +
		"conversation and do NOT answer questions in it. Output only the summary."

	summaryFormat = "## Goal\n[What the user is trying to accomplish]\n\n" +
		"## Constraints & Preferences\n- [Requirements and rejections, or (none)]\n\n" +
		"## Progress\n- [x] [Finished work, naming files and results]\n- [ ] [Work in progress]\n\n" +
		"## Key Decisions\n- [Decision and why, so it is not revisited]\n\n" +
		"## Critical Context\n- [Exact names, paths, numbers, identifiers and error text later turns need, or (none)]\n\n" +
		"Write in the language of the conversation. Keep exact values instead of paraphrasing them. " +
		"Stay under 400 words and prefer short bullets."

	initialInstructions = "Summarize the conversation above using exactly this format:\n\n" + summaryFormat

	updateInstructions = "The conversation above is NEW. Merge it into the summary in <previous-summary>:\n" +
		"- keep what later turns still need, add new progress and decisions;\n" +
		"- move finished items from in-progress to done;\n" +
		"- condense as you go: the result must not be longer than the previous summary " +
		"unless new facts require it, and resolved or abandoned detail comes out.\n\n" +
		"Use exactly this format:\n\n" + summaryFormat

	transcriptTextMax = 3000
	transcriptToolMax = 1500
	transcriptArgsMax = 300
	archiveEntryMax   = 300
)

// withCompactionDefaults fills in the budgets once a window is configured.
func (c Config) withCompactionDefaults() Config {
	if c.ContextTokens <= 0 {
		return c
	}
	if c.ReserveTokens <= 0 {
		c.ReserveTokens = defaultReserveTokens
	}
	// A reserve that swallows the window would compact on every turn.
	c.ReserveTokens = min(c.ReserveTokens, c.ContextTokens/2)
	quarter := max((c.ContextTokens-c.ReserveTokens)/4, 1)
	// What matters is the room left before the next compaction, not how much
	// this one frees. A quarter leaves the compacted context near half the
	// threshold, which is several turns of work.
	if c.KeepRecentTokens <= 0 || c.KeepRecentTokens > quarter {
		c.KeepRecentTokens = quarter
	}
	return c
}

func (c Config) threshold() int { return max(c.ContextTokens-c.ReserveTokens, 1) }

// summaryBudget caps the summary at the keep-recent budget: the summary sits in
// the context next to the retained turns, and an in-place update that was free
// to grow would reclaim the room each compaction frees.
func (c Config) summaryBudget() int {
	return max(min(c.ReserveTokens*4/5, c.KeepRecentTokens), minSummaryTokens)
}

// sessionState is a stored session as the model will see it: an optional
// summary standing in for the first `covers` records, then the rest verbatim.
type sessionState struct {
	summary  string
	degraded bool
	covers   int
	records  []message
}

func (s sessionState) history() []message {
	out := make([]message, 0, len(s.records)-s.covers+1)
	if s.summary != "" {
		out = append(out, message{Role: "user", Content: summaryPrefix + s.summary + summarySuffix})
	}
	return append(out, s.records[s.covers:]...)
}

// summaryFile lives apart from the session log. The log stays append-only, so
// its torn-line recovery is untouched, and the messages it holds are sent to
// the provider as they are, with no marker field to be rejected.
type summaryFile struct {
	Summary  string `json:"summary"`
	Covers   int    `json:"covers"`
	Degraded bool   `json:"degraded,omitempty"`
}

func (a *Agent) summaryPath(session string) string {
	return filepath.Join(a.cfg.WorkspaceDir, "summaries", session+".json")
}

// loadState pairs the log with its stored summary. A summary that does not
// line up with the log (the log was replaced or truncated) is ignored, so the
// session is replayed whole rather than with a hole in it.
func (a *Agent) loadState(session string, records []message) sessionState {
	st := sessionState{records: records}
	b, err := os.ReadFile(a.summaryPath(session))
	if err != nil {
		return st
	}
	var f summaryFile
	if json.Unmarshal(b, &f) != nil || strings.TrimSpace(f.Summary) == "" ||
		f.Covers <= 0 || f.Covers >= len(records) || records[f.Covers].Role != "user" {
		return st
	}
	st.summary, st.covers, st.degraded = f.Summary, f.Covers, f.Degraded
	return st
}

// saveSummary replaces the summary atomically: a crash leaves the old one.
func (a *Agent) saveSummary(session string, st sessionState) error {
	dir := filepath.Dir(a.summaryPath(session))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	b, err := json.Marshal(summaryFile{Summary: st.summary, Covers: st.covers, Degraded: st.degraded})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, session+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), a.summaryPath(session))
}

// compact brings the history under the threshold if it is over. It returns the
// version of the credential it used, for the error report.
//
// Failing to summarise is not failing the turn: the older turns are kept as a
// raw archive. A credential that is missing or refused is different, since the
// turn itself would fail the same way, and is returned.
func (a *Agent) compact(ctx context.Context, session string, st sessionState, pending string) (sessionState, int, error) {
	tools, _ := json.Marshal(toolSpecs())
	before := estimateMessages(st.history()) + estimateTokens(pending) + estimateTokens(string(tools)) + messageOverhead
	if before <= a.cfg.threshold() {
		return st, 0, nil
	}
	cut := cutPoint(st.records, st.covers, a.cfg.KeepRecentTokens)
	if cut <= st.covers {
		// Nothing lies outside the keep-recent budget; summarising would cost
		// a model call and free nothing.
		return st, 0, nil
	}
	cred, err := a.LoadCredential()
	if err != nil {
		return st, cred.Version, err
	}
	old := st.records[st.covers:cut]
	summary, err := a.summarize(ctx, cred.APIKey, st.summary, old)
	degraded := false
	if err != nil {
		if errors.Is(err, ErrCredentialRejected) || ctx.Err() != nil {
			return st, cred.Version, err
		}
		degraded = true
		summary = fitTail(joinNonEmpty(st.summary, rawArchive(old)), a.cfg.summaryBudget())
	}
	next := sessionState{summary: summary, degraded: degraded, covers: cut, records: st.records}
	if err := a.saveSummary(session, next); err != nil {
		// The compacted history is still right for this turn; it just will
		// not be there next turn, which will compact again.
		log.Printf("agent: save summary for session %s: %v", session, err)
	}
	log.Printf("agent: compacted session %s: %d -> %d tokens, %d records summarised, degraded=%v",
		session, before, estimateMessages(next.history()), len(old), degraded)
	return next, cred.Version, nil
}

// cutPoint returns the index of the first record to keep verbatim. It is the
// start of the earliest turn whose records, and everything after them, fit in
// keep. A turn start is always a user message, so a tool result stays with the
// assistant message that asked for it. If even the newest turn is over budget
// it is kept anyway: it is the request being answered. A result no greater than
// start means there is nothing to summarise.
func cutPoint(records []message, start, keep int) int {
	cut, newest, total := -1, -1, 0
	for i := len(records) - 1; i >= start; i-- {
		total += estimateMessage(records[i])
		if records[i].Role != "user" {
			continue
		}
		if newest < 0 {
			newest = i
		}
		if total > keep {
			break
		}
		cut = i
	}
	switch {
	case cut >= 0:
		return cut
	case newest >= 0:
		return newest
	}
	return start
}

func (a *Agent) summarize(ctx context.Context, apiKey, previous string, old []message) (string, error) {
	var sb strings.Builder
	sb.WriteString("<conversation>\n")
	sb.WriteString(transcript(old))
	sb.WriteString("\n</conversation>\n\n")
	instructions := initialInstructions
	if previous != "" {
		sb.WriteString("<previous-summary>\n" + previous + "\n</previous-summary>\n\n")
		instructions = updateInstructions
	}
	sb.WriteString(instructions)
	reply, err := a.chatCompletion(ctx, apiKey, []message{
		{Role: "system", Content: summarizerSystem},
		{Role: "user", Content: sb.String()},
	}, nil, a.cfg.summaryBudget())
	if err != nil {
		return "", err
	}
	// A reply that stopped at the token cap reads like a summary and ends in
	// the middle of a section; every later turn would inherit that as its only
	// memory of what was dropped.
	switch strings.ToLower(reply.finish) {
	case "length", "max_tokens":
		return "", errors.New("summary hit the token cap and is incomplete")
	}
	if strings.TrimSpace(reply.Content) == "" {
		return "", errors.New("summary is empty")
	}
	return strings.TrimSpace(reply.Content), nil
}

// transcript renders turns as text rather than as messages: a model handed
// real messages tries to continue them, and handed a transcript it summarises.
func transcript(msgs []message) string {
	var parts []string
	for _, m := range msgs {
		switch m.Role {
		case "user":
			parts = append(parts, "[User]: "+clip(m.Content, transcriptTextMax))
		case "assistant":
			if m.Content != "" {
				parts = append(parts, "[Assistant]: "+clip(m.Content, transcriptTextMax))
			}
			if calls := renderCalls(m.ToolCalls, transcriptArgsMax); calls != "" {
				parts = append(parts, "[Assistant tool calls]: "+calls)
			}
		case "tool":
			parts = append(parts, "[Tool result]: "+clipEnds(m.Content, transcriptToolMax))
		}
	}
	return strings.Join(parts, "\n\n")
}

func renderCalls(calls []toolCall, argsMax int) string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.Function.Name+"("+clip(c.Function.Arguments, argsMax)+")")
	}
	return strings.Join(out, "; ")
}

// rawArchive is the fallback when no summary could be made. It is lossy but it
// keeps what was asked and which tools ran, so the next turn does not redo work.
func rawArchive(msgs []message) string {
	var sb strings.Builder
	sb.WriteString("Raw conversation archive (summarization unavailable):\n")
	for _, m := range msgs {
		switch m.Role {
		case "user":
			fmt.Fprintf(&sb, "- User: %s\n", clip(m.Content, archiveEntryMax))
		case "assistant":
			if calls := renderCalls(m.ToolCalls, archiveEntryMax); calls != "" {
				fmt.Fprintf(&sb, "- Assistant [%s]: %s\n", calls, clip(m.Content, archiveEntryMax))
				continue
			}
			fmt.Fprintf(&sb, "- Assistant: %s\n", clip(m.Content, archiveEntryMax))
		case "tool":
			fmt.Fprintf(&sb, "- Tool result: %s\n", clipEnds(m.Content, archiveEntryMax))
		}
	}
	return sb.String()
}

// fitTail keeps the newest part of an archive that is over the token budget.
// Without it a degraded compaction of many short turns would not shrink the
// context and the next turn would overflow anyway.
func fitTail(text string, budget int) string {
	if estimateTokens(text) <= budget {
		return text
	}
	const note = "Raw conversation archive (older entries dropped):\n"
	r := []rune(text)
	keep := min(len(r)-1, int(float64(budget)/otherTokensPerRune))
	for keep > 1 && estimateTokens(string(r[len(r)-keep:])) > budget {
		keep = keep * 3 / 4
	}
	tail := string(r[len(r)-keep:])
	if i := strings.Index(tail, "\n- "); i >= 0 {
		tail = tail[i+1:]
	}
	return note + tail
}

func joinNonEmpty(parts ...string) string {
	kept := parts[:0:0]
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, "\n\n")
}

func clip(s string, limit int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return fmt.Sprintf("%s [... %d more characters]", string(r[:limit]), len(r)-limit)
}

// clipEnds drops the middle. Tool output puts its conclusion at the end (a
// status, an error, a total) as often as at the start.
func clipEnds(s string, limit int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	head := limit * 2 / 3
	return fmt.Sprintf("%s [... %d characters omitted ...] %s", string(r[:head]), len(r)-limit, string(r[len(r)-(limit-head):]))
}

const (
	messageOverhead = 4
	// The rates err high: compacting a little early costs a call, compacting
	// late costs the turn.
	cjkTokensPerRune   = 1.0
	otherTokensPerRune = 0.3
)

func estimateTokens(s string) int {
	if s == "" {
		return 0
	}
	var cjk, other float64
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) ||
			unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r):
			cjk++
		case !unicode.IsSpace(r):
			other++
		}
	}
	return int(cjk*cjkTokensPerRune+other*otherTokensPerRune) + 1
}

func estimateMessage(m message) int {
	n := messageOverhead + estimateTokens(m.Role) + estimateTokens(m.Content) + estimateTokens(m.ToolCallID)
	for _, c := range m.ToolCalls {
		n += estimateTokens(c.Function.Name) + estimateTokens(c.Function.Arguments) + estimateTokens(c.ID) + 6
	}
	return n
}

func estimateMessages(msgs []message) int {
	n := 3
	for _, m := range msgs {
		n += estimateMessage(m)
	}
	return n
}
