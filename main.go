package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	_ "github.com/google/uuid"
)

type Customer struct {
	ID        string `gorm:"column:id;primaryKey"`
	Name      string
	IsDeleted bool
}

type Account struct {
	ID            string `gorm:"column:id;primaryKey"`
	AccountNumber int64
	OwnerId       string
	Amount        float64
	OpenDate      *time.Time
	IsDeleted     bool
}

type Transaction struct {
	ID                            string `gorm:"column:id;primaryKey"`
	SenderAccountId               string
	SenderAccountAccountNumber    int64
	RecipientAccountId            string
	RecipientAccountAccountNumber int64
	Amount                        float64
	TransactionStatusId           int
	CreatedDate                   time.Time
}

type TransactionStatus struct {
	ID          int `gorm:"column:id;primaryKey"`
	Name        string
	Description string
}

func main() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/customer/{customerid}/openaccount/{startamount}", openAccount)

	fmt.Println("Server started on :8080")
	http.ListenAndServe(":8080", r)
}

func openAccount(w http.ResponseWriter, r *http.Request) {
	// Извлекаем параметры из URL
	customerID := chi.URLParam(r, "customerid")
	startAmountStr := chi.URLParam(r, "startamount")

	// Преобразуем startAmount из строки в float64
	startAmount, err := strconv.ParseFloat(startAmountStr, 64)
	if err != nil {
		http.Error(w, "Invalid start amount", http.StatusBadRequest)
		return
	}

	// Открываем соединение с базой данных
	db, err := sql.Open("postgres", "host=localhost user=postgres password=postgres dbname=transactions sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Вызываем функцию OpenAccount
	account, err := OpenAccount(db, customerID, startAmount)
	if err != nil {
		http.Error(w, "Error opening account", http.StatusInternalServerError)
		return
	}

	// Преобразуем account в JSON и отправляем ответ
	accountJSON, err := json.Marshal(account)
	if err != nil {
		http.Error(w, "Error encoding account to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(accountJSON)
}

func OpenAccount(db *sql.DB, customerID string, startAmount float64) (*Account, error) {
	// Проверяем, существует ли клиент
	var customer Customer
	err := db.QueryRow(`SELECT "id", "Name", "IsDeleted" FROM "Customers" WHERE "id" = $1`, customerID).Scan(&customer.ID, &customer.Name, &customer.IsDeleted)
	if err != nil {
		if err == sql.ErrNoRows {
			// Клиент не найден
			return nil, fmt.Errorf("customer with ID %s does not exist", customerID)
		}
		// Другая ошибка при выполнении запроса
		return nil, err
	}

	// Проверяем, есть ли у пользователя закрытый счет
	var closedAccount Account
	err = db.QueryRow(`SELECT * FROM "Accounts" WHERE "OwnerId" = $1 AND "IsDeleted" = $2`, customerID, true).Scan(&closedAccount)
	if err != nil && err != sql.ErrNoRows {
		// Ошибка при выполнении запроса
		return nil, err
	}

	now := time.Now()
	var account *Account
	if err == nil {
		// Если есть закрытый счет, переоткрываем его
		account = &closedAccount
		account.IsDeleted = false
		account.Amount = startAmount
		account.OpenDate = &now

		_, err = db.Exec(`UPDATE "Accounts" SET "IsDeleted" = $1, "Amount" = $2, "OpenDate" = $3 WHERE "id" = $4`, account.IsDeleted, account.Amount, account.OpenDate, account.ID)
		if err != nil {
			// Ошибка при обновлении счета
			return nil, err
		}
	} else {
		// Если нет закрытого счета, создаем новый
		var lastNumber int64
		err = db.QueryRow(`SELECT MAX("AccountNumber") FROM "Accounts"`).Scan(&lastNumber)
		if err != nil && err != sql.ErrNoRows {
			lastNumber = 7770000
		}

		account = &Account{
			ID:            uuid.New().String(),
			AccountNumber: lastNumber + 1,
			OwnerId:       customerID,
			Amount:        startAmount,
			OpenDate:      &now,
			IsDeleted:     false,
		}

		_, err = db.Exec(`INSERT INTO "Accounts" (id, "AccountNumber", "OwnerId", "Amount", "OpenDate", "IsDeleted") VALUES ($1, $2, $3, $4, $5, $6)`, account.ID, account.AccountNumber, customer.ID, account.Amount, account.OpenDate, account.IsDeleted)
		if err != nil {
			// Ошибка при создании счета
			return nil, err
		}
	}

	return account, nil
}
