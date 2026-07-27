package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

type ModelPricing struct {
	InputCostPer1M  float64
	OutputCostPer1M float64
}

var pricingTable = map[string]ModelPricing{
	"claude-opus-4-8":     {InputCostPer1M: 15.00, OutputCostPer1M: 75.00},
	"claude-sonnet-5":     {InputCostPer1M: 3.00, OutputCostPer1M: 15.00},
	"claude-fable-5":      {InputCostPer1M: 0.50, OutputCostPer1M: 1.50},
	"claude-4.8-thinking": {InputCostPer1M: 5.00, OutputCostPer1M: 20.00},
	"gpt-5.6-sol":         {InputCostPer1M: 2.50, OutputCostPer1M: 10.00},
	"qwen-3.6-coder":      {InputCostPer1M: 0.20, OutputCostPer1M: 0.80},
}

type UserKey struct {
	Key        string  `json:"key"`
	Balance    float64 `json:"balance"`
	TotalSpent float64 `json:"total_spent"`
	IsActive   bool    `json:"is_active"`
}

type KeyStore struct {
	Keys map[string]*UserKey `json:"keys"`
}

type KeyManager struct {
	mu   sync.Mutex
	data *KeyStore
	path string
}

var keyMgr *KeyManager

func initKeyManager() {
	km, err := NewKeyManager("/tmp/keys.json")
	if err != nil {
		log.Printf("[WARN] Failed to initialize key store, using in-memory only: %v", err)
		km = &KeyManager{data: &KeyStore{Keys: make(map[string]*UserKey)}, path: ""}
	}
	keyMgr = km
	keyMgr.AddBalance("sk-kiit-test-key-12345", 10.00)
	log.Printf("[INFO] Key manager initialized with test key balance: $10.00")
}

func NewKeyManager(path string) (*KeyManager, error) {
	data := &KeyStore{Keys: make(map[string]*UserKey)}
	if f, err := os.ReadFile(path); err == nil {
		json.Unmarshal(f, data)
	}
	return &KeyManager{data: data, path: path}, nil
}

func (km *KeyManager) save() {
	if km.path == "" {
		return
	}
	f, _ := json.MarshalIndent(km.data, "", "  ")
	os.WriteFile(km.path, f, 0644)
}

func (km *KeyManager) AuthenticateAndCheckBalance(rawKey string) (*UserKey, error) {
	km.mu.Lock()
	defer km.mu.Unlock()

	uk, ok := km.data.Keys[rawKey]
	if !ok {
		return nil, errors.New("invalid_api_key")
	}
	if !uk.IsActive {
		return nil, errors.New("key_disabled")
	}
	if uk.Balance <= 0.0 {
		return nil, errors.New("insufficient_balance")
	}
	return uk, nil
}

func CalculateCost(virtualModel string, promptTokens, completionTokens int) float64 {
	pricing, ok := pricingTable[virtualModel]
	if !ok {
		pricing = pricingTable["claude-opus-4-8"]
	}
	inputCost := (float64(promptTokens) / 1000000.0) * pricing.InputCostPer1M
	outputCost := (float64(completionTokens) / 1000000.0) * pricing.OutputCostPer1M
	return inputCost + outputCost
}

func (km *KeyManager) DeductBalance(rawKey string, cost float64) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	uk, ok := km.data.Keys[rawKey]
	if !ok {
		return errors.New("key_not_found")
	}
	uk.Balance -= cost
	uk.TotalSpent += cost
	km.save()
	return nil
}

func (km *KeyManager) AddBalance(rawKey string, amount float64) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	if uk, ok := km.data.Keys[rawKey]; ok {
		uk.Balance += amount
	} else {
		km.data.Keys[rawKey] = &UserKey{
			Key:      rawKey,
			Balance:  amount,
			IsActive: true,
		}
	}
	km.save()
	return nil
}

func writeError(w http.ResponseWriter, statusCode int, errCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"message": message,
			"type":    "invalid_request_error",
			"code":    errCode,
		},
	})
}

func adminAddFundsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Admin-Secret") != adminSecret {
		writeError(w, http.StatusUnauthorized, "unauthorized", "Invalid admin secret")
		return
	}
	var req struct {
		Key    string  `json:"key"`
		Amount float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_payload", "Invalid request payload")
		return
	}
	if err := keyMgr.AddBalance(req.Key, req.Amount); err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", "Failed to update database")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf(`{"status":"success","key":"%s","added_amount":%.2f}`, req.Key, req.Amount)))
}
