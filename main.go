package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultPort = "8787"
	opencodeURL = "https://opencode.ai/zen/v1/chat/completions"
	targetModel = "deepseek-v4-flash-free"
)

var internalSecret string

type ChatRequest struct {
	Model    string          `json:"model"`
	Messages json.RawMessage `json:"messages"`
	Stream   bool            `json:"stream,omitempty"`
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

	log.Printf("[INFO] Go Engine active on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[FATAL] Server crash: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"running","engine":"go-native"}`))
}

func modelsHandler(w http.ResponseWriter, r *http.Request) {
	modelsResponse := map[string]interface{}{
		"object": "list",
		"data": []map[string]interface{}{
			{"id": "gpt-5.6-sol", "object": "model", "owned_by": "kiitcode-ultra"},
			{"id": "claude-3-7-sonnet", "object": "model", "owned_by": "kiitcode-ultra"},
			{"id": "claude-3-5-opus", "object": "model", "owned_by": "kiitcode-ultra"},
			{"id": "deepseek-v4-flash", "object": "model", "owned_by": "kiitcode-free"},
			{"id": "qwen-3.6-coder", "object": "model", "owned_by": "kiitcode-ultra"},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(modelsResponse)
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
	if err := json.Unmarshal(bodyBytes, &reqPayload); err == nil {
		reqPayload.Model = targetModel
		bodyBytes, _ = json.Marshal(reqPayload)
	}

	client := &http.Client{Timeout: 120 * time.Second}
	req, err := http.NewRequest(http.MethodPost, opencodeURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		http.Error(w, `{"error":"Internal dispatch creation failure"}`, http.StatusInternalServerError)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Dispatch error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"Upstream provider unreachable"}`))
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}
