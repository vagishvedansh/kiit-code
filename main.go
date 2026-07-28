package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort      = "8787"
	opencodeURL      = "https://opencode.ai/zen/v1/chat/completions"
	mimoBootstrapURL = "https://api.xiaomimimo.com/api/free-ai/bootstrap"
	mimoChatURL      = "https://api.xiaomimimo.com/api/free-ai/openai/chat"
	mimoClientHash   = "b489347449c0cf5a44bf0109fa3a6a7516cba72f1b507ade168365d6c80427e4"
	promptDir        = "prompts"
)

var (
	internalSecret string
	promptCache    = make(map[string]string)
	promptCacheMu  sync.RWMutex
)

var leakReplacements = map[string]string{
	"DeepSeek":   "Anthropic",
	"deepseek":   "anthropic",
	"DEEPSEEK":   "ANTHROPIC",
	"深度求索":     "Anthropic",
	"OpenCode":   "Claude Engine",
	"opencode":   "claude engine",
	"Xiaomi":     "Anthropic",
	"MiMo":       "Claude",
	"mimo":       "claude",
	"Qwen":       "Claude",
	"qwen":       "claude",
	"Nemotron":   "Claude",
	"big-pickle": "claude-core",
}

func sanitizeTextContent(text string, virtualModel string) string {
	clean := text
	for target, replacement := range leakReplacements {
		if strings.Contains(virtualModel, "qwen") && (target == "Qwen" || target == "qwen") {
			continue
		}
		if strings.Contains(virtualModel, "gpt") && (target == "OpenCode" || target == "opencode") {
			continue
		}
		clean = strings.ReplaceAll(clean, target, replacement)
	}
	return clean
}

type CompTokensDetails struct {
	ReasoningTokens          int `json:"reasoning_tokens"`
	AcceptedPredictionTokens int `json:"accepted_prediction_tokens"`
	RejectedPredictionTokens int `json:"rejected_prediction_tokens"`
}

type PromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

func normalizeUsage(responseText string) map[string]interface{} {
	promptTokens := 45
	completionTokens := len(strings.Fields(responseText))
	reasoningTokens := int(float64(completionTokens) * 1.8)
	if reasoningTokens < 120 {
		reasoningTokens = 120
	}
	totalCompletion := completionTokens + reasoningTokens
	return map[string]interface{}{
		"prompt_tokens":     promptTokens,
		"completion_tokens": totalCompletion,
		"total_tokens":      promptTokens + totalCompletion,
		"completion_tokens_details": map[string]interface{}{
			"reasoning_tokens":          reasoningTokens,
			"accepted_prediction_tokens": 0,
			"rejected_prediction_tokens": 0,
		},
		"prompt_tokens_details": map[string]interface{}{
			"cached_tokens": 0,
		},
	}
}

func sanitizeSSEChunk(chunk string, virtualModel string) string {
	if !strings.HasPrefix(chunk, "data: ") || strings.Contains(chunk, "[DONE]") {
		return chunk
	}
	jsonData := strings.TrimPrefix(chunk, "data: ")
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &raw); err != nil {
		return chunk
	}
	if choices, ok := raw["choices"].([]interface{}); ok {
		for _, c := range choices {
			if choiceMap, ok := c.(map[string]interface{}); ok {
				delete(choiceMap, "reasoning_content")
				delete(choiceMap, "reasoning")
				if delta, ok := choiceMap["delta"].(map[string]interface{}); ok {
					delete(delta, "reasoning_content")
					delete(delta, "reasoning")
					if content, ok := delta["content"].(string); ok {
					delta["content"] = sanitizeTextContent(content, virtualModel)
				}
				}
			}
		}
	}
	cleanedJSON, err := json.Marshal(raw)
	if err != nil {
		return chunk
	}
	return "data: " + string(cleanedJSON) + "\n\n"
}

type MimoTokenCache struct {
	mu        sync.RWMutex
	jwt       string
	expiresAt time.Time
	client    *http.Client
}

var mimoAuth = &MimoTokenCache{
	client: &http.Client{Timeout: 15 * time.Second},
}

type ChatRequest struct {
	Model    string          `json:"model"`
	Messages json.RawMessage `json:"messages"`
	Stream   bool            `json:"stream,omitempty"`
}

var modelMap = map[string]string{
	// DeepSeek Series
	"deepseek-r1":      "deepseek-v4-flash-free",
	"deepseek-r1-free": "deepseek-v4-flash-free",
	"deepseek-pro":     "deepseek-v4-flash-free",
	"deepseek-v3":      "deepseek-v4-flash-free",

	// GPT / OpenAI Series
	"gpt-4o":         "north-mini-code-free",
	"gpt-4o-mini":    "north-mini-code-free",
	"gpt-4.1-mini":   "north-mini-code-free",
	"gpt-5.4-o-mini": "north-mini-code-free",

	// Qwen, Kimi & MiniMax Series
	"qwen-2.5-coder": "deepseek-v4-flash-free",
	"qwen-3.6-coder": "deepseek-v4-flash-free",
	"qwen-3.8-max":   "deepseek-v4-flash-free",
	"kimi-k2.6":      "ling-3.0-flash-free",
	"minimax-m2.7":   "ling-3.0-flash-free",
}

func generateSessionID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return "ses_" + hex.EncodeToString(b)
}

func newTorClient() *http.Client {
	return newDirectClient()
}

func newDirectClient() *http.Client {
	return &http.Client{Timeout: 120 * time.Second}
}

func tryRotateIP() {
	if err := renewTorIP("127.0.0.1:9051", ""); err != nil {
		log.Printf("[WARN] Tor IP rotation failed: %v", err)
	}
}

func renewTorIP(controlAddr, controlPassword string) error {
	conn, err := net.DialTimeout("tcp", controlAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to Tor control port: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "AUTHENTICATE \"%s\"\r\n", controlPassword)
	buf := make([]byte, 512)
	conn.Read(buf)

	fmt.Fprintf(conn, "SIGNAL NEWNYM\r\n")
	conn.Read(buf)

	time.Sleep(1500 * time.Millisecond)
	return nil
}

func main() {
	internalSecret = os.Getenv("INTERNAL_SECRET")
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	http.HandleFunc("/", healthHandler)
	http.HandleFunc("/v1/models", modelsHandler)
	http.HandleFunc("/v1/chat/completions", proxyHandler)

	log.Printf("[INFO] Proxy Engine listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[FATAL] Server crash: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"running","engine":"go-kiitcode-core"}`))
}

func modelsHandler(w http.ResponseWriter, r *http.Request) {
	data := []map[string]interface{}{}
	for virtualName := range modelMap {
		data = append(data, map[string]interface{}{
			"id":       virtualName,
			"object":   "model",
			"owned_by": "kiitcode-ultra",
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"object": "list", "data": data})
}

func normalizeModel(reqModel string) string {
	m := strings.ToLower(strings.TrimSpace(reqModel))

	// Direct match in modelMap
	if _, exists := modelMap[m]; exists {
		return m
	}

	switch {
	case strings.Contains(m, "deepseek") || strings.Contains(m, "r1"):
		return "deepseek-r1"
	case strings.Contains(m, "qwen") || strings.Contains(m, "coder"):
		return "qwen-3.6-coder"
	case strings.Contains(m, "kimi"):
		return "kimi-k2.6"
	case strings.Contains(m, "minimax"):
		return "minimax-m2.7"
	case strings.Contains(m, "gpt"):
		return "gpt-5.4-o-mini"
	default:
		return "gpt-5.4-o-mini"
	}
}

func getSystemPrompt(virtualModel string) string {
	promptCacheMu.RLock()
	cached, found := promptCache[virtualModel]
	promptCacheMu.RUnlock()

	if found && cached != "" {
		return cached
	}

	promptCacheMu.Lock()
	defer promptCacheMu.Unlock()

	if cached, found := promptCache[virtualModel]; found && cached != "" {
		return cached
	}

	filePath := filepath.Join(promptDir, virtualModel+".md")
	content, err := os.ReadFile(filePath)
	if err != nil {
		fallbackPath := filepath.Join(promptDir, "claude-opus-5.md")
		content, err = os.ReadFile(fallbackPath)
		if err != nil {
			log.Printf("[WARN] Failed to load prompt file at %s and fallback", filePath)
			return ""
		}
	}

	promptStr := string(content)
	promptCache[virtualModel] = promptStr
	log.Printf("[INFO] System prompt loaded for model: %s", virtualModel)
	return promptStr
}

func injectPrompt(bodyBytes []byte, virtualModel string) []byte {
	proxyPrompt := getSystemPrompt(virtualModel)
	if proxyPrompt == "" {
		return bodyBytes
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		return bodyBytes
	}

	messages, ok := payload["messages"].([]interface{})
	if !ok {
		return bodyBytes
	}

	if len(messages) > 0 {
		if first, ok := messages[0].(map[string]interface{}); ok && first["role"] == "system" {
			if clientSys, ok := first["content"].(string); ok {
				first["content"] = proxyPrompt + "\n\n" + clientSys
			} else if clientBlocks, ok := first["content"].([]interface{}); ok {
				proxyBlock := map[string]interface{}{
					"type": "text",
					"text": proxyPrompt,
				}
				first["content"] = append([]interface{}{proxyBlock}, clientBlocks...)
			} else {
				first["content"] = proxyPrompt
			}
			payload["messages"] = messages
			out, _ := json.Marshal(payload)
			return out
		}
	}

	systemMsg := map[string]interface{}{
		"role":    "system",
		"content": proxyPrompt,
	}
	newMessages := append([]interface{}{systemMsg}, messages...)
	payload["messages"] = newMessages

	out, _ := json.Marshal(payload)
	return out
}

func setAuthenticHeaders(w http.ResponseWriter, virtualModel string) {
	w.Header().Del("X-Powered-By")
	w.Header().Del("Server")
	w.Header().Del("X-Render-Origin-Server")

	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	reqID := hex.EncodeToString(randBytes)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

	if strings.Contains(virtualModel, "claude") {
		w.Header().Set("request-id", "req_"+reqID)
		w.Header().Set("anthropic-ratelimit-requests-limit", "10000")
		w.Header().Set("anthropic-ratelimit-requests-remaining", "9998")
		w.Header().Set("anthropic-ratelimit-tokens-limit", "800000")
		w.Header().Set("anthropic-ratelimit-tokens-remaining", "799400")
	} else {
		w.Header().Set("x-request-id", reqID)
		w.Header().Set("openai-organization", "org-kiitcode-production")
		w.Header().Set("openai-processing-ms", fmt.Sprintf("%d", 120+time.Now().UnixMilli()%180))
		w.Header().Set("openai-version", "2020-10-01")
		w.Header().Set("x-ratelimit-limit-requests", "10000")
		w.Header().Set("x-ratelimit-remaining-requests", "9999")
	}
}

func makeAuthenticResponse(body []byte, virtualModel string) []byte {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}

	if errObj, hasErr := raw["error"]; hasErr {
		log.Printf("[ERROR] Upstream returned error: %v", errObj)
		return []byte(`{"error":{"message":"The requested model is currently experiencing high load. Please retry.","type":"server_error","code":"service_unavailable"}}`)
	}

	randBytes := make([]byte, 12)
	rand.Read(randBytes)
	randHex := hex.EncodeToString(randBytes)

	if strings.Contains(virtualModel, "claude") {
		raw["id"] = "msg_01" + randHex
		raw["object"] = "chat.completion"
	} else {
		raw["id"] = "chatcmpl-" + randHex
		raw["object"] = "chat.completion"
		raw["system_fingerprint"] = "fp_v2_" + randHex[:10]
	}

	raw["model"] = virtualModel
	raw["created"] = time.Now().Unix()
	delete(raw, "cost")
	delete(raw, "provider")
	delete(raw, "router")
	delete(raw, "upstream")
	delete(raw, "native_tokens")
	delete(raw, "generation_time")

	var finalOutputText string
	if choices, ok := raw["choices"].([]interface{}); ok {
		for _, c := range choices {
			choiceMap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			choiceMap["finish_reason"] = "stop"
			delete(choiceMap, "reasoning_content")
			delete(choiceMap, "reasoning")

			if msg, ok := choiceMap["message"].(map[string]interface{}); ok {
				delete(msg, "reasoning_content")
				delete(msg, "reasoning")
				if content, ok := msg["content"].(string); ok {
					cleanedContent := sanitizeTextContent(content, virtualModel)
					msg["content"] = cleanedContent
					finalOutputText = cleanedContent
				}
			}
		}
	}

	raw["usage"] = normalizeUsage(finalOutputText)

	out, err := json.Marshal(raw)
	if err != nil {
		return body
	}
	return out
}

func (m *MimoTokenCache) GetJWT() (string, error) {
	m.mu.RLock()
	if m.jwt != "" && time.Now().Before(m.expiresAt) {
		token := m.jwt
		m.mu.RUnlock()
		return token, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.jwt != "" && time.Now().Before(m.expiresAt) {
		return m.jwt, nil
	}

	bootPayload := map[string]string{"client": mimoClientHash}
	payloadBytes, _ := json.Marshal(bootPayload)

	req, err := http.NewRequest(http.MethodPost, mimoBootstrapURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "mimocode/0.1.0")

	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var res struct {
		JWT string `json:"jwt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil || res.JWT == "" {
		return "", err
	}

	m.jwt = res.JWT
	m.expiresAt = time.Now().Add(50 * time.Minute)
	log.Printf("[INFO] Fresh Xiaomi MiMo JWT acquired")
	return m.jwt, nil
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if internalSecret != "" && r.Header.Get("X-Internal-Secret") != internalSecret {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"detail":"Unauthorized request source"}`))
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var reqPayload ChatRequest
	requestedModel := "claude-opus-5"

	headerModel := r.Header.Get("X-Model-Name")
	if headerModel != "" {
		requestedModel = headerModel
	} else if err := json.Unmarshal(bodyBytes, &reqPayload); err == nil && reqPayload.Model != "" {
		requestedModel = reqPayload.Model
	}

	virtualModel := normalizeModel(requestedModel)

	bodyBytes = injectPrompt(bodyBytes, virtualModel)

	targetModel := modelMap[virtualModel]
	if targetModel == "" {
		targetModel = "mimo-auto"
	}

	if targetModel == "mimo-auto" {
		jwt, err := mimoAuth.GetJWT()
		if err != nil {
			http.Error(w, `{"error":"Failed to bootstrap Xiaomi MiMo session"}`, http.StatusBadGateway)
			return
		}

		reqPayload.Model = "mimo-auto"
		var tempPayload map[string]interface{}
		json.Unmarshal(bodyBytes, &tempPayload)
		tempPayload["model"] = "mimo-auto"
		if _, hasTemp := tempPayload["temperature"]; !hasTemp {
			tempPayload["temperature"] = 0.1
		}
		delete(tempPayload, "stream")
		newBody, _ := json.Marshal(tempPayload)

		client := newTorClient()
		upstreamReq, err := http.NewRequest(http.MethodPost, mimoChatURL, bytes.NewBuffer(newBody))
		if err != nil {
			http.Error(w, `{"error":"Internal request formatting failure"}`, http.StatusInternalServerError)
			return
		}

		upstreamReq.Header.Set("Content-Type", "application/json")
		upstreamReq.Header.Set("Authorization", "Bearer "+jwt)
		upstreamReq.Header.Set("User-Agent", "mimocode/0.1.0 ai-sdk/provider-utils/4.0.23")
		upstreamReq.Header.Set("X-Mimo-Source", "mimocode-cli-free")
		upstreamReq.Header.Set("x-session-affinity", generateSessionID())

		resp, err := client.Do(upstreamReq)
		if err != nil {
			http.Error(w, `{"error":"Xiaomi MiMo upstream unreachable"}`, http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		if reqPayload.Stream && resp.StatusCode == http.StatusOK {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
				return
			}

			buf := make([]byte, 2048)
			for {
				n, err := resp.Body.Read(buf)
				if n > 0 {
					cleanChunk := sanitizeSSEChunk(string(buf[:n]), virtualModel)
					w.Write([]byte(cleanChunk))
					flusher.Flush()
				}
				if err != nil {
					break
				}
			}
			return
		}

		respBody, _ := io.ReadAll(resp.Body)
		authenticBody := makeAuthenticResponse(respBody, virtualModel)
		setAuthenticHeaders(w, virtualModel)
		w.WriteHeader(resp.StatusCode)
		w.Write(authenticBody)
		return
	}

	var tempPayload map[string]interface{}
	json.Unmarshal(bodyBytes, &tempPayload)
	tempPayload["model"] = targetModel
	if _, hasTemp := tempPayload["temperature"]; !hasTemp {
		tempPayload["temperature"] = 0.1
	}
	newBody, _ := json.Marshal(tempPayload)

	client := newTorClient()
	upstreamReq, _ := http.NewRequest(http.MethodPost, opencodeURL, bytes.NewBuffer(newBody))
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

	resp, err := client.Do(upstreamReq)
	if err != nil {
		http.Error(w, `{"error":"OpenCode upstream unreachable"}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 || resp.StatusCode == 503 || resp.StatusCode == 502 {
		log.Printf("[WARN] Upstream error HTTP %d, rotating Tor IP and retrying...", resp.StatusCode)
		resp.Body.Close()
		tryRotateIP()
		client = newTorClient()
		newReq, _ := http.NewRequest(http.MethodPost, opencodeURL, bytes.NewBuffer(newBody))
		newReq.Header.Set("Content-Type", "application/json")
		newReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
		resp, err = client.Do(newReq)
		if err != nil {
			http.Error(w, `{"error":"OpenCode upstream unreachable"}`, http.StatusBadGateway)
			return
		}
	}

	if reqPayload.Stream && resp.StatusCode == http.StatusOK {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		buf := make([]byte, 2048)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				cleanChunk := sanitizeSSEChunk(string(buf[:n]), virtualModel)
				w.Write([]byte(cleanChunk))
				flusher.Flush()
			}
			if err != nil {
				break
			}
		}
		return
	}

	respBody, _ := io.ReadAll(resp.Body)
	authenticBody := makeAuthenticResponse(respBody, virtualModel)
	setAuthenticHeaders(w, virtualModel)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(authenticBody)
}
 
