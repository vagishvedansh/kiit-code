package main

import "testing"

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
	for _, want := range []string{"claude-3-5-sonnet-20241022", "Anthropic", "identity_guard", "environment variables"} {
		if !contains(g, want) {
			t.Errorf("guard missing %q", want)
		}
	}
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
