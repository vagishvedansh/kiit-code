package main

import (
	"strings"
	"testing"
)

// TestAdversarialReasoningFilter_SplitChunkBoundaries rigorously tests
// StreamingReasoningFilter with aggressive, adversarial chunk splits.
func TestAdversarialReasoningFilter_SplitChunkBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		chunks   []string
		expected string
	}{
		{
			name: "single-byte chunking of think tag",
			chunks: []string{
				"<", "t", "h", "i", "n", "k", ">",
				"s", "e", "c", "r", "e", "t", " ", "t", "h", "o", "u", "g", "h", "t", "s",
				"<", "/", "t", "h", "i", "n", "k", ">",
				"H", "e", "l", "l", "o",
			},
			expected: "Hello",
		},
		{
			name: "split opening tag across two chunks",
			chunks: []string{
				"Prefix ",
				"<th",
				"ink>Reasoning here</think>",
				"Suffix",
			},
			expected: "Prefix Suffix",
		},
		{
			name: "split closing tag across two chunks",
			chunks: []string{
				"<think>Reasoning here</th",
				"ink>Clean text",
			},
			expected: "Clean text",
		},
		{
			name: "split both opening and closing across chunks",
			chunks: []string{
				"Start: ",
				"<th",
				"ink>hidden</",
				"think>",
				"Done",
			},
			expected: "Start: Done",
		},
		{
			name: "thought tags with split",
			chunks: []string{
				"<thou",
				"ght>internal</thought",
				">visible",
			},
			expected: "visible",
		},
		{
			name: "reasoning tags with split",
			chunks: []string{
				"<reas",
				"oning>thought</reason",
				"ing>visible",
			},
			expected: "visible",
		},
		{
			name: "reflection tags with split",
			chunks: []string{
				"<reflec",
				"tion>thought</reflection",
				">visible",
			},
			expected: "visible",
		},
		{
			name: "identity_guard tags with split",
			chunks: []string{
				"<identity_",
				"guard>hidden</identity_guard",
				">visible",
			},
			expected: "visible",
		},
		{
			name: "multiple interleaved reasoning blocks",
			chunks: []string{
				"A ",
				"<think>T1</think>",
				"B ",
				"<think>T2</think>",
				"C",
			},
			expected: "A B C",
		},
		{
			name: "legitimate C++ include with split angle bracket",
			chunks: []string{
				"#include <",
				"iostream>",
				"\nint main() {}",
			},
			expected: "#include <iostream>\nint main() {}",
		},
		{
			name: "legitimate comparison operators in code",
			chunks: []string{
				"if (x <",
				" 5 && y > 10) { return; }",
			},
			expected: "if (x < 5 && y > 10) { return; }",
		},
		{
			name: "legitimate template type in C++",
			chunks: []string{
				"std::vector<",
				"int> numbers;",
			},
			expected: "std::vector<int> numbers;",
		},
		{
			name: "unterminated reasoning tag at stream EOF (must be suppressed)",
			chunks: []string{
				"Normal prefix. ",
				"<think>unfinished thought at EOF",
			},
			expected: "Normal prefix. ",
		},
		{
			name: "trailing incomplete tag buffer flushed safely",
			chunks: []string{
				"Result: x <",
			},
			expected: "Result: x <",
		},
		{
			name: "angle bracket immediately followed by letters that are not tags",
			chunks: []string{
				"<div class='foo'>hello</div>",
			},
			expected: "<div class='foo'>hello</div>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := NewStreamingReasoningFilter("claude-3-5-sonnet")
			var sb strings.Builder
			for _, chunk := range tc.chunks {
				res := filter.Feed(chunk)
				sb.WriteString(res)
			}
			sb.WriteString(filter.Flush())

			got := sb.String()
			if got != tc.expected {
				t.Errorf("Test %q failed:\n  got:      %q\n  expected: %q", tc.name, got, tc.expected)
			}
		})
	}
}

// Fuzz-like permutation test: tests arbitrary cut positions across reasoning tags
func TestAdversarialReasoningFilter_AllCutPositions(t *testing.T) {
	original := "Normal text before <think>Secret thoughts that must be removed</think> Normal text after."
	expected := "Normal text before  Normal text after."

	for cut := 1; cut < len(original); cut++ {
		filter := NewStreamingReasoningFilter("claude-3-5-sonnet")
		part1 := original[:cut]
		part2 := original[cut:]

		res := filter.Feed(part1)
		res += filter.Feed(part2)
		res += filter.Flush()

		if res != expected {
			t.Errorf("Cut at %d failed:\n  part1: %q\n  part2: %q\n  got:   %q\n  want:  %q",
				cut, part1, part2, res, expected)
		}
	}
}
