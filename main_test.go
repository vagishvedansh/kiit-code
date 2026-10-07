package main

import (
	"regexp"
	"testing"
)

func TestContentToString(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"array_of_blocks", []interface{}{map[string]interface{}{"type": "text", "text": "a"}, map[string]interface{}{"type": "text", "text": "b"}}, "ab"},
		{"empty_array", []interface{}{}, ""},
	}
	for _, c := range cases {
		if got := contentToString(c.in); got != c.want {
			t.Errorf("%s: contentToString(%v) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

func TestExtractOpenAIContent(t *testing.T) {
	// 1. normal string content
	if got := extractOpenAIContent([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi there"}}]}`)); got != "hi there" {
		t.Errorf("string content: got %q", got)
	}
	// 2. content is null but reasoning_content has the answer (the content:null root cause)
	got := extractOpenAIContent([]byte(`{"choices":[{"message":{"role":"assistant","content":null,"reasoning_content":"the real answer"}}]}`))
	if got != "the real answer" {
		t.Errorf("null-content+reasoning fallback: got %q, want %q", got, "the real answer")
	}
	// 3. content as array of blocks
	got = extractOpenAIContent([]byte(`{"choices":[{"message":{"role":"assistant","content":[{"type":"text","text":"block-"},{"type":"text","text":"A"}]}}]}`))
	if got != "block-A" {
		t.Errorf("array content: got %q", got)
	}
	// 4. no choices
	if got := extractOpenAIContent([]byte(`{"foo":"bar"}`)); got != "" {
		t.Errorf("no choices: got %q", got)
	}
	// 5. invalid json
	if got := extractOpenAIContent([]byte(`not json`)); got != "" {
		t.Errorf("invalid json: got %q", got)
	}
}

func TestIsSupportedModel(t *testing.T) {
	known := []string{"gpt-4o", "claude-sonnet-4-5", "claude-3-5-sonnet-20241022", "deepseek-r1"}
	for _, m := range known {
		if !isSupportedModel(m) {
			t.Errorf("expected %q to be supported", m)
		}
	}
	unknown := []string{"", "nonexistent-model-xyz-999", "fake-gpt-9000"}
	for _, m := range unknown {
		if isSupportedModel(m) {
			t.Errorf("expected %q to be unsupported (should 404)", m)
		}
	}
}

func TestThinkingRequested(t *testing.T) {
	if thinkingRequested(nil) {
		t.Error("nil thinking should be false")
	}
	if thinkingRequested(map[string]interface{}{"type": "disabled"}) {
		t.Error("type disabled should be false")
	}
	if !thinkingRequested(map[string]interface{}{"type": "enabled", "budget_tokens": 1024}) {
		t.Error("type enabled should be true")
	}
	if !thinkingRequested(map[string]interface{}{"enabled": true}) {
		t.Error("enabled bool true should be true")
	}
}

func TestIdentityGuardPrompt(t *testing.T) {
	g := identityGuardPrompt("claude-3-5-sonnet-20241022")
	for _, want := range []string{"Claude 3.5 Sonnet", "Anthropic", "identity_guard", "environment variables"} {
		if !contains(g, want) {
			t.Errorf("guard missing %q", want)
		}
	}
}

func TestSanitizeTextContent_OxAlpha(t *testing.T) {
	in := "I'm ox-alpha, an LLM developed by an undisclosed organization."
	got := sanitizeTextContent(in, "claude-3-opus-20240229")
	want := "I'm Claude 3 Opus, an LLM developed by Anthropic."
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	in2 := "Hello! I am ox-alpha from an undisclosed company."
	got2 := sanitizeTextContent(in2, "claude-opus-5")
	want2 := "Hello! I am Claude Opus 5 from Anthropic."
	if got2 != want2 {
		t.Errorf("got %q, want %q", got2, want2)
	}

	// Verify whitespace is preserved in chunks
	chunk := " world, how are you?"
	gotChunk := sanitizeTextContent(chunk, "claude-3-opus-20240229")
	if gotChunk != chunk {
		t.Errorf("got chunk %q, want %q", gotChunk, chunk)
	}
}

func TestStreamingWhitespacePreservation(t *testing.T) {
	re := regexp.MustCompile(`\S+\s*|\s+`)
	full := "Hello there, I am Claude 3 Opus! How can I help you today?"
	parts := re.FindAllString(full, -1)
	var reconstructed string
	for _, p := range parts {
		reconstructed += p
	}
	if reconstructed != full {
		t.Errorf("reconstructed %q != full %q", reconstructed, full)
	}
}

func TestTracePipeline(t *testing.T) {
	raw := []byte(`{"id":"202608260235431bb08777c4b644b9","object":"chat.completion","created":1787682945,"model":"x-preview-f-free","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"I'm ox-alpha, a large language model developed by an undisclosed organization.","reasoning_content":"The user is asking who I am."}}]}`)
	extracted := extractOpenAIContent(raw)
	t.Logf("extracted: %q", extracted)
	cleaned := cleanOutputText(extracted, "claude-3-opus-20240229")
	t.Logf("cleaned: %q", cleaned)
	re := regexp.MustCompile(`\S+\s*|\s+`)
	parts := re.FindAllString(cleaned, -1)
	t.Logf("parts: %v", parts)
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestGenerateRequestID(t *testing.T) {
	id1 := generateRequestID()
	id2 := generateRequestID()

	if !regexp.MustCompile(`^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$`).MatchString(id1) {
		t.Errorf("generateRequestID() format mismatch: %q", id1)
	}
	if id1 == id2 {
		t.Errorf("expected consecutive request IDs to be unique: %q == %q", id1, id2)
	}
}

func TestGenerateSessionID(t *testing.T) {
	for i := 0; i < 20; i++ {
		sess := generateSessionID()
		if !isValidOpenCodeSession(sess) {
			t.Errorf("generateSessionID() produced invalid session: %q", sess)
		}
	}
}

func TestIsValidOpenCodeSession(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"random-uuid-1234", false},
		{"ses_short", false},
		{"ses_f1ca452fdffe1IvfaQCvkIzXHe", true},
		{"msg_f0f66a50bffeB7MDxc270HJi55", true},
	}
	for _, c := range cases {
		if got := isValidOpenCodeSession(c.in); got != c.want {
			t.Errorf("isValidOpenCodeSession(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestGetCandidateModels_NemotronExclusionFixed(t *testing.T) {
	// Crucial M2 check: nemotron-3.5-lightning-free must NOT exclude fallbacks
	candidates := getCandidateModels("nemotron-3.5-lightning-free")
	if len(candidates) <= 1 {
		t.Fatalf("expected multiple candidates for nemotron, got %v", candidates)
	}
	if candidates[0] != "nemotron-3.5-lightning-free" {
		t.Errorf("expected primary model first, got %q", candidates[0])
	}

	foundMuse := false
	foundPreview := false
	for _, m := range candidates {
		if m == "muse-spark-1.3-contributor-free" {
			foundMuse = true
		}
		if m == "x-preview-f-free" {
			foundPreview = true
		}
	}
	if !foundMuse {
		t.Errorf("expected candidate chain to include muse-spark-1.3-contributor-free: %v", candidates)
	}
	if !foundPreview {
		t.Errorf("expected candidate chain to include x-preview-f-free: %v", candidates)
	}

	// Verify deduplication
	seen := make(map[string]bool)
	for _, m := range candidates {
		if seen[m] {
			t.Errorf("duplicate model %q in candidate chain: %v", m, candidates)
		}
		seen[m] = true
	}
}

func TestGetIsolatedTorProxyURL(t *testing.T) {
	u1 := getIsolatedTorProxyURL()
	u2 := getIsolatedTorProxyURL()

	if u1 == u2 {
		t.Errorf("expected distinct per-request SOCKS auth credentials, got identical: %q", u1)
	}
	if !regexp.MustCompile(`^socks5://tor_\d+_[0-9a-f]+:isolate@.+`).MatchString(u1) {
		t.Errorf("unexpected isolated proxy URL pattern: %q", u1)
	}
}

func TestExtractSSEText_ContentPartDelta(t *testing.T) {
	raw := []byte("data: {\"type\":\"response.content_part.delta\",\"delta\":{\"text\":\"hello from content part\"}}\n\n")
	got := extractSSEText(raw)
	if got != "hello from content part" {
		t.Errorf("extractSSEText with content_part.delta got %q, want %q", got, "hello from content part")
	}
}

