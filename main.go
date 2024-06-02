package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
	"log"
	"time"
)

type TransactionCreatedMessage struct {
	Id               string `json:"Id"`
	SenderAccount    string `json:"SenderAccount"`
	RecipientAccount string `json:"RecipientAccount"`
	Amount           string `json:"Amount"`
	Timestamp        string `json:"timestamp"`
}

type Transaction struct {
	ID                  string    `db:"id"`
	SenderAccount       int64     `db:"SenderAccountAccountNumber"`
	RecipientAccount    int64     `db:"RecipientAccountAccountNumber"`
	Amount              float64   `db:"Amount"`
	TransactionStatusId int8      `db:"TransactionStatusId"`
	CreatedDate         time.Time `db:"CreatedDate"`
}

type Account struct {
	ID            string    `db:"id"`
	AccountNumber int64     `db:"AccountNumber"`
	OwnerId       string    `db:"OwnerId"`
	Amount        float64   `db:"Amount"`
	IsDeleted     bool      `db:"IsDeleted"`
	OpenDate      time.Time `db:"OpenDate"`
}

func main() {
	//CGO_ENABLED 1
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "consumer-group-3",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"TransactionsCreated"}, nil)

	db, err := sql.Open("postgres", "host=localhost user=postgres password=postgres dbname=transactions sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			var tr TransactionCreatedMessage
			json.Unmarshal(msg.Value, &tr)
			err = MakeTransaction(db, tr)
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err)
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	c.Close()
}

func MakeTransaction(db *sql.DB, tr TransactionCreatedMessage) error {
	// Fetch transaction from DB
	var trDb Transaction
	err := db.QueryRow(`SELECT t.id, t."SenderAccountAccountNumber", t."RecipientAccountAccountNumber", t."Amount", t."TransactionStatusId", t."CreatedDate" FROM "Transactions" AS t WHERE id = $1`, tr.Id).Scan(&trDb.ID, &trDb.SenderAccount, &trDb.RecipientAccount, &trDb.Amount, &trDb.TransactionStatusId, &trDb.CreatedDate)
	if err != nil {
		return err
	}

	// Update transaction status to Processing in DB
	_, err = db.Exec(`UPDATE "Transactions" SET "TransactionStatusId" = 2 WHERE id = $1`, trDb.ID)
	if err != nil {
		return err
	}

	// Fetch sender and recipient accounts from DB
	var senderAccount, recipientAccount Account
	err = db.QueryRow(`SELECT a.id, a."AccountNumber", a."OwnerId", a."Amount", a."IsDeleted", a."OpenDate" FROM "Accounts" AS a WHERE a."AccountNumber" = $1 AND a."IsDeleted" = false`, trDb.SenderAccount).Scan(&senderAccount.ID, &senderAccount.AccountNumber, &senderAccount.OwnerId, &senderAccount.Amount, &senderAccount.IsDeleted, &senderAccount.OpenDate)
	if err != nil {
		return err
	}
	err = db.QueryRow(`SELECT a.id, a."AccountNumber", a."OwnerId", a."Amount", a."IsDeleted", a."OpenDate" FROM "Accounts" AS a WHERE a."AccountNumber" = $1 AND a."IsDeleted" = false`, trDb.RecipientAccount).Scan(&recipientAccount.ID, &recipientAccount.AccountNumber, &recipientAccount.OwnerId, &recipientAccount.Amount, &recipientAccount.IsDeleted, &recipientAccount.OpenDate)
	if err != nil {
		return err
	}

	// Perform transaction
	senderRemains, recipientRemains, err := PerformTransaction(senderAccount, recipientAccount, trDb.Amount)
	if err != nil {
		// Update transaction status to Cancelled in DB
		_, err = db.Exec(`UPDATE "Transactions" SET "TransactionStatusId" = 4 WHERE id = $1`, trDb.ID)
		return err
	}

	// Update transaction status to Completed in DB
	_, err = db.Exec(`UPDATE "Transactions" SET "TransactionStatusId" = 3 WHERE id = $1`, trDb.ID)
	if err != nil {
		return err
	}

	// Update sender and recipient account amounts in DB
	_, err = db.Exec(`UPDATE "Accounts" SET "Amount" = $1 WHERE "AccountNumber" = $2`, senderRemains, trDb.SenderAccount)
	if err != nil {
		return err
	}
	_, err = db.Exec(`UPDATE "Accounts" SET "Amount" = $1 WHERE "AccountNumber" = $2`, recipientRemains, trDb.RecipientAccount)
	return err
}

func PerformTransaction(senderAccount Account, recipientAccount Account, amount float64) (senderRemains float64, recipientRemains float64, err error) {
	// Decrease sender's account
	senderRemains = senderAccount.Amount - amount
	if senderRemains < 0 {
		return 0, 0, errors.New("insufficient funds")
	}

	// Increase recipient's account
	recipientRemains = recipientAccount.Amount + amount

	return senderRemains, recipientRemains, nil
}
