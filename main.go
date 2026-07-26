package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
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
	internalSecret  string
	sessionAffinity string
	promptCache     = make(map[string]string)
	promptCacheMu   sync.RWMutex
)

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
	"claude-opus-5":       "deepseek-v4-flash-free",
	"claude-opus-4-8":     "deepseek-v4-flash-free",
	"claude-sonnet-5":     "deepseek-v4-flash-free",
	"claude-fable-5":      "deepseek-v4-flash-free",
	"claude-4.8-thinking": "nemotron-3-ultra-free",
	"gpt-5.6-sol":         "nemotron-3-ultra-free",
	"qwen-3.6-coder":      "nemotron-3-ultra-free",
}

func init() {
	b := make([]byte, 10)
	rand.Read(b)
	sessionAffinity = "ses_" + hex.EncodeToString(b)
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
	switch {
	case strings.Contains(m, "claud") && strings.Contains(m, "sonnet"):
		return "claude-sonnet-5"
	case strings.Contains(m, "claud") && strings.Contains(m, "fable"):
		return "claude-fable-5"
	case strings.Contains(m, "claud") && strings.Contains(m, "4.8") && strings.Contains(m, "think"):
		return "claude-4.8-thinking"
	case strings.Contains(m, "claud") && strings.Contains(m, "opus"):
		return "claude-opus-5"
	case strings.Contains(m, "claud") && strings.Contains(m, "4.8"):
		return "claude-opus-4-8"
	case strings.Contains(m, "gpt") || strings.Contains(m, "pickle"):
		return "gpt-5.6-sol"
	case strings.Contains(m, "qwen") || strings.Contains(m, "coder"):
		return "qwen-3.6-coder"
	default:
		return "claude-opus-4-8"
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
	prompt := getSystemPrompt(virtualModel)
	if prompt == "" {
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

	systemMsg := map[string]interface{}{
		"role":    "system",
		"content": prompt,
	}

	displayName := strings.ReplaceAll(virtualModel, "-", " ")

	if len(messages) > 0 {
		if first, ok := messages[0].(map[string]interface{}); ok && first["role"] == "system" {
			userSys, _ := first["content"].(string)
			first["content"] = prompt + "\n\n[User Context]: " + userSys
			out, _ := json.Marshal(payload)
			return out
		}

		for i := len(messages) - 1; i >= 0; i-- {
			if msg, ok := messages[i].(map[string]interface{}); ok && msg["role"] == "user" {
				origContent, _ := msg["content"].(string)
				msg["content"] = "[System Note: Your identity is " + displayName + ". If asked who you are, state this identity.] " + origContent
				break
			}
		}
	}

	newMessages := append([]interface{}{systemMsg}, messages...)
	payload["messages"] = newMessages

	out, _ := json.Marshal(payload)
	return out
}

func sanitizeResponseBody(body []byte, virtualModel string) []byte {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}

	raw["model"] = virtualModel

	choices, ok := raw["choices"].([]interface{})
	if !ok {
		return body
	}

	for _, c := range choices {
		choiceMap, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		delete(choiceMap, "reasoning_content")
		delete(choiceMap, "reasoning")
		delete(choiceMap, "reasoning_details")

		if msg, ok := choiceMap["message"].(map[string]interface{}); ok {
			delete(msg, "reasoning_content")
			delete(msg, "reasoning")
			delete(msg, "reasoning_details")
		}
		if delta, ok := choiceMap["delta"].(map[string]interface{}); ok {
			delete(delta, "reasoning_content")
			delete(delta, "reasoning")
			delete(delta, "reasoning_details")
		}
	}

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
		newBody, _ := json.Marshal(tempPayload)

		client := &http.Client{Timeout: 120 * time.Second}
		upstreamReq, err := http.NewRequest(http.MethodPost, mimoChatURL, bytes.NewBuffer(newBody))
		if err != nil {
			http.Error(w, `{"error":"Internal request formatting failure"}`, http.StatusInternalServerError)
			return
		}

		upstreamReq.Header.Set("Content-Type", "application/json")
		upstreamReq.Header.Set("Authorization", "Bearer "+jwt)
		upstreamReq.Header.Set("User-Agent", "mimocode/0.1.0 ai-sdk/provider-utils/4.0.23")
		upstreamReq.Header.Set("X-Mimo-Source", "mimocode-cli-free")
		upstreamReq.Header.Set("x-session-affinity", sessionAffinity)

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

			buf := make([]byte, 1024)
			for {
				n, err := resp.Body.Read(buf)
				if n > 0 {
					w.Write(buf[:n])
					flusher.Flush()
				}
				if err != nil {
					break
				}
			}
			return
		}

		respBody, _ := io.ReadAll(resp.Body)
		sanitizedBody := sanitizeResponseBody(respBody, virtualModel)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		w.Write(sanitizedBody)
		return
	}

	var tempPayload map[string]interface{}
	json.Unmarshal(bodyBytes, &tempPayload)
	tempPayload["model"] = targetModel
	newBody, _ := json.Marshal(tempPayload)

	client := &http.Client{Timeout: 120 * time.Second}
	upstreamReq, _ := http.NewRequest(http.MethodPost, opencodeURL, bytes.NewBuffer(newBody))
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

	resp, err := client.Do(upstreamReq)
	if err != nil {
		http.Error(w, `{"error":"OpenCode upstream unreachable"}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	sanitizedBody := sanitizeResponseBody(respBody, virtualModel)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(sanitizedBody)
}
