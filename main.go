package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Account struct {
	ID            string
	AccountNumber int64
	OwnerId       string
	Amount        float64
	OpenDate      *time.Time
	IsDeleted     bool
}

var accounts = make(map[string]*Account)
var accountNumber int64 = 7770000
var mutex = &sync.Mutex{}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/customer/{customerid}/openaccount/{startamount}", openAccount)
	r.Post("/customer/{customerid}/closeaccount", closeAccount)
	r.Post("/customer/{customerid}/reopenaccount/{startamount}", reopenAccount)

	fmt.Println("Server started on :8080")
	http.ListenAndServe(":8080", r)
}

func openAccount(w http.ResponseWriter, r *http.Request) {
	customerID := chi.URLParam(r, "customerid")
	startAmountStr := chi.URLParam(r, "startamount")

	startAmount, err := strconv.ParseFloat(startAmountStr, 64)
	if err != nil {
		http.Error(w, "Invalid start amount", http.StatusBadRequest)
		return
	}

	mutex.Lock()
	account, err := OpenAccount(customerID, startAmount)
	mutex.Unlock()
	if err != nil {
		http.Error(w, "Error opening account", http.StatusInternalServerError)
		return
	}

	accountJSON, err := json.Marshal(account)
	if err != nil {
		http.Error(w, "Error encoding account to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(accountJSON)
}

func closeAccount(w http.ResponseWriter, r *http.Request) {
	customerID := chi.URLParam(r, "customerid")

	mutex.Lock()
	err := CloseAccount(customerID)
	mutex.Unlock()
	if err != nil {
		http.Error(w, "Error closing account", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func reopenAccount(w http.ResponseWriter, r *http.Request) {
	customerID := chi.URLParam(r, "customerid")
	startAmountStr := chi.URLParam(r, "startamount")

	startAmount, err := strconv.ParseFloat(startAmountStr, 64)
	if err != nil {
		http.Error(w, "Invalid start amount", http.StatusBadRequest)
		return
	}

	mutex.Lock()
	account, err := ReopenAccount(customerID, startAmount)
	mutex.Unlock()
	if err != nil {
		http.Error(w, "Error reopening account", http.StatusInternalServerError)
		return
	}

	accountJSON, err := json.Marshal(account)
	if err != nil {
		http.Error(w, "Error encoding account to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(accountJSON)
}

func OpenAccount(customerID string, startAmount float64) (*Account, error) {
	if _, ok := accounts[customerID]; ok {
		return nil, fmt.Errorf("account with ID %s already exists", customerID)
	}

	now := time.Now()
	account := &Account{
		ID:            uuid.New().String(),
		AccountNumber: accountNumber,
		OwnerId:       customerID,
		Amount:        startAmount,
		OpenDate:      &now,
		IsDeleted:     false,
	}

	accountNumber++
	accounts[customerID] = account

	return account, nil
}

func CloseAccount(customerID string) error {
	account, ok := accounts[customerID]
	if !ok {
		return fmt.Errorf("account with ID %s does not exist", customerID)
	}

	account.IsDeleted = true
	return nil
}

func ReopenAccount(customerID string, startAmount float64) (*Account, error) {
	account, ok := accounts[customerID]
	if !ok {
		return nil, fmt.Errorf("account with ID %s does not exist", customerID)
	}

	if !account.IsDeleted {
		return nil, fmt.Errorf("account with ID %s is not closed", customerID)
	}

	now := time.Now()
	account.IsDeleted = false
	account.Amount = startAmount
	account.OpenDate = &now

	return account, nil
}
