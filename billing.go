package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	_ "modernc.org/sqlite"
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
	Key        string
	Balance    float64
	TotalSpent float64
	IsActive   bool
}

type KeyManager struct {
	db *sql.DB
	mu sync.Mutex
}

var keyMgr *KeyManager

func initKeyManager() {
	var err error
	keyMgr, err = NewKeyManager("keys.db")
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize SQLite key database: %v", err)
	}
	keyMgr.AddBalance("sk-kiit-test-key-12345", 10.00)
}

func NewKeyManager(dbPath string) (*KeyManager, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	query := `CREATE TABLE IF NOT EXISTS api_keys (
		key TEXT PRIMARY KEY,
		balance REAL NOT NULL DEFAULT 0.0,
		total_spent REAL NOT NULL DEFAULT 0.0,
		is_active INTEGER NOT NULL DEFAULT 1
	);`
	if _, err := db.Exec(query); err != nil {
		return nil, err
	}
	return &KeyManager{db: db}, nil
}

func (km *KeyManager) AuthenticateAndCheckBalance(rawKey string) (*UserKey, error) {
	km.mu.Lock()
	defer km.mu.Unlock()

	var uk UserKey
	var active int
	err := km.db.QueryRow("SELECT key, balance, total_spent, is_active FROM api_keys WHERE key = ?", rawKey).
		Scan(&uk.Key, &uk.Balance, &uk.TotalSpent, &active)
	if err == sql.ErrNoRows {
		return nil, errors.New("invalid_api_key")
	} else if err != nil {
		return nil, err
	}
	uk.IsActive = active == 1
	if !uk.IsActive {
		return nil, errors.New("key_disabled")
	}
	if uk.Balance <= 0.0 {
		return nil, errors.New("insufficient_balance")
	}
	return &uk, nil
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
	tx, err := km.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE api_keys SET balance = balance - ?, total_spent = total_spent + ? WHERE key = ?", cost, cost, rawKey)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (km *KeyManager) AddBalance(rawKey string, amount float64) error {
	km.mu.Lock()
	defer km.mu.Unlock()
	_, err := km.db.Exec("INSERT INTO api_keys (key, balance, total_spent, is_active) VALUES (?, ?, 0.0, 1) ON CONFLICT(key) DO UPDATE SET balance = balance + ?", rawKey, amount, amount)
	return err
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
