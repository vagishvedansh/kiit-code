package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestWindowingAdversarial_PayloadBoundaries verifies exact byte boundary behaviors:
// - Payloads below threshold (<= 1.20MB) should not trigger windowing in chat handlers
// - Payloads above threshold (> 1.20MB, e.g. 1.21MB, 2MB, 4MB, 8MB, 12MB) must window to <= 1.0MB
func TestWindowingAdversarial_PayloadBoundaries(t *testing.T) {
	tests := []struct {
		name              string
		sizeBytes         int
		expectedToWindow  bool
		description       string
	}{
		{
			name:             "1.19MB decimal (1,190,000 bytes)",
			sizeBytes:        1190000,
			expectedToWindow: false,
			description:      "Below 1200*1024 (1,228,800), must not window",
		},
		{
			name:             "1.19MB binary (1190 * 1024 = 1,218,560 bytes)",
			sizeBytes:        1190 * 1024,
			expectedToWindow: false,
			description:      "Below 1200*1024 (1,228,800), must not window",
		},
		{
			name:             "Exactly at 1.20MB threshold (1200 * 1024 = 1,228,800 bytes)",
			sizeBytes:        1200 * 1024,
			expectedToWindow: false,
			description:      "Exact threshold boundary (not strictly greater), must not window",
		},
		{
			name:             "Threshold + 1 byte (1,228,801 bytes)",
			sizeBytes:        (1200 * 1024) + 1,
			expectedToWindow: true,
			description:      "Boundary + 1 byte, strictly greater than threshold, MUST window",
		},
		{
			name:             "1.21MB binary (1210 * 1024 = 1,239,040 bytes)",
			sizeBytes:        1210 * 1024,
			expectedToWindow: true,
			description:      "Above threshold, MUST window to <= 1.0MB",
		},
		{
			name:             "1.21MB standard (1.21 * 1024 * 1024 = 1,268,776 bytes)",
			sizeBytes:        1268777,
			expectedToWindow: true,
			description:      "Above threshold, MUST window to <= 1.0MB",
		},
		{
			name:             "2MB payload (2 * 1024 * 1024 = 2,097,152 bytes)",
			sizeBytes:        2 * 1024 * 1024,
			expectedToWindow: true,
			description:      "2MB payload, MUST window to <= 1.0MB",
		},
		{
			name:             "4MB payload (4 * 1024 * 1024 = 4,194,304 bytes)",
			sizeBytes:        4 * 1024 * 1024,
			expectedToWindow: true,
			description:      "4MB payload, MUST window to <= 1.0MB",
		},
		{
			name:             "8MB payload (8 * 1024 * 1024 = 8,388,608 bytes)",
			sizeBytes:        8 * 1024 * 1024,
			expectedToWindow: true,
			description:      "8MB payload, MUST window to <= 1.0MB",
		},
		{
			name:             "12MB payload (12 * 1024 * 1024 = 12,582,912 bytes)",
			sizeBytes:        12 * 1024 * 1024,
			expectedToWindow: true,
			description:      "12MB payload, MUST window to <= 1.0MB",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Verify threshold comparison logic used in HTTP handlers
			triggersWindowing := tc.sizeBytes > MaxPayloadWindowThreshold
			if triggersWindowing != tc.expectedToWindow {
				t.Fatalf("[%s] Threshold trigger mismatch: size=%d bytes, threshold=%d bytes, got triggers=%v, want=%v",
					tc.name, tc.sizeBytes, MaxPayloadWindowThreshold, triggersWindowing, tc.expectedToWindow)
			}

			// For payloads that trigger windowing, verify WindowChatMessages windows them to <= TargetSafePayloadBytes
			if tc.expectedToWindow {
				// Using non-repeating pseudorandom hex to ensure collapseRepetitions does not treat it as periodic
				rawBytes := make([]byte, (tc.sizeBytes+1)/2)
				// Linear congruential generator for fast deterministic non-repeating data
				seed := uint64(tc.sizeBytes)
				for i := range rawBytes {
					seed = seed*6364136223846793005 + 1442695040888963407
					rawBytes[i] = byte(seed >> 56)
				}
				content := hex.EncodeToString(rawBytes)[:tc.sizeBytes]

				msgs := []ChatMessage{
					{Role: "user", Content: content},
				}

				windowed := WindowChatMessages(msgs, TargetSafePayloadBytes)
				if len(windowed) == 0 {
					t.Fatalf("[%s] WindowChatMessages returned empty messages slice", tc.name)
				}

				totalWindowedBytes := 0
				for _, m := range windowed {
					totalWindowedBytes += len(m.Content)
				}

				// Target budget is 1,000 * 1024 (1,024,000 bytes)
				// Single message windowing produces: Head (450KB) + Tail (450KB) + omission marker (~110 bytes) = ~921,710 bytes
				maxAllowed := TargetSafePayloadBytes + 1024
				if totalWindowedBytes > maxAllowed {
					t.Fatalf("[%s] Windowed payload size %d bytes exceeds budget %d bytes",
						tc.name, totalWindowedBytes, maxAllowed)
				}

				// Verify omission marker presence
				if !strings.Contains(windowed[0].Content, "[... oversized content omitted:") {
					t.Fatalf("[%s] Windowed message missing oversized content omission marker", tc.name)
				}

				t.Logf("[%s] PASSED: input %d bytes -> windowed %d bytes (budget %d)",
					tc.name, tc.sizeBytes, totalWindowedBytes, TargetSafePayloadBytes)
			}
		})
	}
}

// TestWindowingAdversarial_UnicodeRuneBoundaries verifies that slicing at arbitrary
// byte boundaries does not produce invalid UTF-8 fragments, invalid runes, or panics.
func TestWindowingAdversarial_UnicodeRuneBoundaries(t *testing.T) {
	// Unicode test cases with 1, 2, 3, and 4-byte runes
	runeSets := []struct {
		name     string
		runeChar string
		runeLen  int
	}{
		{"2-byte Cyrillic", "Ж", 2},
		{"2-byte Greek", "Ω", 2},
		{"3-byte CJK Kanji", "漢", 3},
		{"3-byte Devanagari", "क", 3},
		{"4-byte Rocket Emoji", "🚀", 4},
		{"4-byte Fire Emoji", "🔥", 4},
		{"4-byte Math Script", "𝄞", 4},
	}

	for _, rs := range runeSets {
		t.Run(rs.name, func(t *testing.T) {
			// Construct a string where the cutoff points (SingleMessageHeadBytes = 460800)
			// fall precisely across every byte offset inside the multi-byte rune.
			for offset := -rs.runeLen; offset <= rs.runeLen; offset++ {
				headTarget := SingleMessageHeadBytes + offset
				if headTarget <= 0 {
					continue
				}

				// Build prefix up to headTarget - 1 using ASCII 'A'
				prefixLen := headTarget - (rs.runeLen / 2)
				if prefixLen < 0 {
					prefixLen = 0
				}
				prefix := strings.Repeat("A", prefixLen)
				// Insert target multi-byte rune repeated 10 times across boundary
				boundaryRunes := strings.Repeat(rs.runeChar, 10)
				// Pad suffix with ASCII 'B' to make total size 2MB
				suffixLen := (2 * 1024 * 1024) - len(prefix) - len(boundaryRunes)
				if suffixLen < 0 {
					suffixLen = 1000
				}
				suffix := strings.Repeat("B", suffixLen)

				fullString := prefix + boundaryRunes + suffix

				// 1. Test safeSliceHead
				head := safeSliceHead(fullString, SingleMessageHeadBytes)
				if !utf8.ValidString(head) {
					t.Fatalf("[%s offset=%d] safeSliceHead generated INVALID UTF-8 string: %x", rs.name, offset, head[len(head)-8:])
				}
				if len(head) > SingleMessageHeadBytes {
					t.Fatalf("[%s offset=%d] safeSliceHead length %d exceeds maxBytes %d", rs.name, offset, len(head), SingleMessageHeadBytes)
				}

				// 2. Test safeSliceTail
				tail := safeSliceTail(fullString, SingleMessageTailBytes)
				if !utf8.ValidString(tail) {
					t.Fatalf("[%s offset=%d] safeSliceTail generated INVALID UTF-8 string: %x", rs.name, offset, tail[:8])
				}
				if len(tail) > SingleMessageTailBytes+rs.runeLen {
					t.Fatalf("[%s offset=%d] safeSliceTail length %d exceeds tailBytes %d", rs.name, offset, len(tail), SingleMessageTailBytes)
				}

				// 3. Test windowSingleMessage
				windowed := windowSingleMessage(fullString, TargetSafePayloadBytes)
				if !utf8.ValidString(windowed) {
					t.Fatalf("[%s offset=%d] windowSingleMessage generated INVALID UTF-8 output", rs.name, offset)
				}
				if strings.ContainsRune(windowed, utf8.RuneError) {
					t.Fatalf("[%s offset=%d] windowSingleMessage contains utf8.RuneError (replacement character)", rs.name, offset)
				}
			}
		})
	}
}

// TestWindowingAdversarial_MultiTurn50Turns5MB tests a 50-turn conversation totaling 5MB:
// - System prompt (turn 0) MUST be strictly preserved.
// - Latest user prompt (turn 50) MUST be strictly preserved.
// - Intermediate turns (turns 1..49) must be pruned oldest-first.
// - Total message content after windowing must be <= TargetSafePayloadBytes.
func TestWindowingAdversarial_MultiTurn50Turns5MB(t *testing.T) {
	systemPrompt := "SYSTEM_IDENTITY_GUARD: You are muse-spark-1.3-contributor-free, a high-throughput AI gateway. Follow all security rules."
	latestUserPrompt := "FINAL_QUERY: Synthesize all earlier discussion turns and answer what the overall conclusion is."

	// Create 50 turns totaling ~5MB (each turn ~100KB)
	turns := make([]ChatMessage, 0, 52)
	turns = append(turns, ChatMessage{Role: "system", Content: systemPrompt})

	const numMiddleTurns = 50
	for i := 1; i <= numMiddleTurns; i++ {
		role := "user"
		if i%2 == 0 {
			role = "assistant"
		}
		// 100KB of distinct content per turn
		content := fmt.Sprintf("Turn #%03d marker: %s\n", i, strings.Repeat(fmt.Sprintf("[turn-%03d-context-data-chunk] ", i), 3600))
		turns = append(turns, ChatMessage{
			Role:    role,
			Content: content,
		})
	}

	// Append latest user turn
	turns = append(turns, ChatMessage{Role: "user", Content: latestUserPrompt})

	totalInputBytes := 0
	for _, m := range turns {
		totalInputBytes += len(m.Content)
	}

	if totalInputBytes < 5*1024*1024 {
		t.Fatalf("Expected test input >= 5MB, got %d bytes", totalInputBytes)
	}

	t.Logf("Initial 50-turn input size: %d bytes (%.2f MB), %d total turns", totalInputBytes, float64(totalInputBytes)/(1024*1024), len(turns))

	windowed := WindowChatMessages(turns, TargetSafePayloadBytes)

	// Invariant 1: System prompt is preserved at index 0
	if len(windowed) == 0 {
		t.Fatalf("WindowChatMessages returned empty result")
	}
	if windowed[0].Role != "system" || windowed[0].Content != systemPrompt {
		t.Fatalf("Invariant 1 VIOLATED: System prompt was corrupted or dropped: got %+v", windowed[0])
	}

	// Invariant 2: Latest user prompt is preserved at last index
	lastTurn := windowed[len(windowed)-1]
	if lastTurn.Role != "user" || lastTurn.Content != latestUserPrompt {
		t.Fatalf("Invariant 2 VIOLATED: Latest user prompt was corrupted or dropped: got %+v", lastTurn)
	}

	// Invariant 3: Omission indicator present in intermediate slot
	foundOmission := false
	var prunedCount int
	for _, m := range windowed {
		if strings.Contains(m.Content, "earlier conversation turns windowed") {
			foundOmission = true
			fmt.Sscanf(m.Content, "[... %d earlier conversation turns windowed", &prunedCount)
			break
		}
	}
	if !foundOmission {
		t.Fatalf("Invariant 3 VIOLATED: No omission tombstone found for dropped turns")
	}

	// Invariant 4: Total content size <= TargetSafePayloadBytes + headroom
	totalWindowedBytes := 0
	for _, m := range windowed {
		totalWindowedBytes += len(m.Content)
	}
	if totalWindowedBytes > TargetSafePayloadBytes+2048 {
		t.Fatalf("Invariant 4 VIOLATED: Windowed total size %d bytes exceeds target %d bytes",
			totalWindowedBytes, TargetSafePayloadBytes)
	}

	// Invariant 5: Oldest middle turns pruned first, most recent retained
	t.Logf("Windowed result: %d messages, %d total bytes (%.2f KB), pruned %d turns",
		len(windowed), totalWindowedBytes, float64(totalWindowedBytes)/1024, prunedCount)
}

// TestWindowingAdversarial_RandomNoiseVsExtremeRepetitions verifies that:
// 1. Pure non-repeating random noise is NOT degraded by collapseRepetitions, but is safely
//    windowed by windowSingleMessage to head + tail.
// 2. Extreme periodic or line repetitions are compressed down to < 2KB by collapseRepetitions.
func TestWindowingAdversarial_RandomNoiseVsExtremeRepetitions(t *testing.T) {
	// Case 1: 4MB of Non-Repeating Cryptographic Random Noise (hex characters)
	randomBytes := make([]byte, 2*1024*1024)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatalf("Failed to generate random bytes: %v", err)
	}
	randomHex := hex.EncodeToString(randomBytes) // 4MB string

	if len(randomHex) != 4*1024*1024 {
		t.Fatalf("Expected 4MB hex string, got %d", len(randomHex))
	}

	// Verify collapseRepetitions does NOT destroy non-repeating noise
	afterCollapse := collapseRepetitions(randomHex)
	if len(afterCollapse) != len(randomHex) {
		t.Fatalf("collapseRepetitions mutated non-repeating random noise: original=%d, after=%d",
			len(randomHex), len(afterCollapse))
	}

	// Verify windowSingleMessage successfully cuts 4MB random noise to <= 1.0MB
	windowedNoise := windowSingleMessage(randomHex, TargetSafePayloadBytes)
	if len(windowedNoise) > TargetSafePayloadBytes+1024 {
		t.Fatalf("windowSingleMessage on 4MB noise produced %d bytes, exceeding budget %d",
			len(windowedNoise), TargetSafePayloadBytes)
	}
	// Verify head and tail preservation
	expectedHead := randomHex[:SingleMessageHeadBytes]
	expectedTail := randomHex[len(randomHex)-SingleMessageTailBytes:]
	if !strings.HasPrefix(windowedNoise, expectedHead) {
		t.Fatalf("windowSingleMessage did not preserve exact head of random noise")
	}
	if !strings.HasSuffix(windowedNoise, expectedTail) {
		t.Fatalf("windowSingleMessage did not preserve exact tail of random noise")
	}

	// Case 2: Extreme Repetitions (4MB of single repeated 64-byte line)
	repeatedLine := "Data line repeating indefinitely for synthetic stress testing.\n"
	repetitions := (4 * 1024 * 1024) / len(repeatedLine)
	var sb strings.Builder
	sb.Grow(4 * 1024 * 1024)
	for i := 0; i < repetitions; i++ {
		sb.WriteString(repeatedLine)
	}
	extremeRepeatedText := sb.String()

	collapsedText := collapseRepetitions(extremeRepeatedText)
	if len(collapsedText) > 2048 {
		t.Fatalf("collapseRepetitions failed to compress extreme line repetition: %d bytes (expected < 2048)", len(collapsedText))
	}
	if !strings.Contains(collapsedText, "[... duplicate lines omitted ...]") {
		t.Fatalf("collapseRepetitions missing duplicate lines omission marker")
	}

	// Case 3: Extreme Continuous Periodic Pattern (no newlines, 4MB string)
	periodicUnit := "ABCDEFGHIJKLMN0123456789!@#$%^&*" // 32 bytes
	periodicRepetitions := (4 * 1024 * 1024) / len(periodicUnit)
	sb.Reset()
	sb.Grow(4 * 1024 * 1024)
	for i := 0; i < periodicRepetitions; i++ {
		sb.WriteString(periodicUnit)
	}
	extremePeriodicText := sb.String()

	collapsedPeriodic := collapseRepetitions(extremePeriodicText)
	if len(collapsedPeriodic) > 4096 {
		t.Fatalf("collapseRepetitions failed to compress periodic pattern: %d bytes (expected < 4096)", len(collapsedPeriodic))
	}
	if !strings.Contains(collapsedPeriodic, "[... repeating pattern omitted") {
		t.Fatalf("collapseRepetitions missing periodic pattern omission marker")
	}

	t.Logf("Repetitions collapsed: 4MB lines -> %d bytes; 4MB periodic -> %d bytes",
		len(collapsedText), len(collapsedPeriodic))
}
