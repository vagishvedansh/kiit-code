package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultPort  = "8787"
	opencodeURL  = "https://opencode.ai/zen/v1/chat/completions"
	torProxyURL  = "socks5://127.0.0.1:9050"
	defaultModel = "deepseek-v4-flash-free"
)

var (
	internalSecret string
)

func main() {
	internalSecret = os.Getenv("INTERNAL_SECRET")
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	http.HandleFunc("/", healthHandler)
	http.HandleFunc("/v1/chat/completions", proxyHandler)

	log.Printf("[INFO] Go Proxy starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[FATAL] Server failed: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"running","engine":"go-native"}`))
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

	client := &http.Client{
		Timeout: 120 * time.Second,
	}

	req, err := http.NewRequest(http.MethodPost, opencodeURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		http.Error(w, `{"error":"Failed to construct upstream request"}`, http.StatusInternalServerError)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Upstream dispatch failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"Failed to reach execution upstream"}`))
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read upstream response"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}
