package main

import (
	"database/sql"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"

	"github.com/michaelbironneau/asbclient"
)

const (
	maxMessageCountToCollectBeforeInsert = 1000
)

type responseMessage struct {
	monitorid  string
	statuscode string
}

var (
	connStr   string = os.Getenv("AZURE_POSTGRES_CONNSTR")
	namespace string = os.Getenv("SERVICE_BUS_NAMESPACE")
	keyValue  string = os.Getenv("SERVICE_BUS_SHARED_ACCESS_KEY_VALUE")

	responseMessages   [maxMessageCountToCollectBeforeInsert]responseMessage
	splittedBusMessage []string

	receivedCount int = 0

	db *sql.DB
	tx *sql.Tx
)

func main() {
	sb := asbclient.New(asbclient.Topic, namespace, "RootManageSharedAccessKey", keyValue)
	sb.SetSubscription("database-inserter")
	for {
		busMessage, _ := sb.PeekLockMessage("response-database-inserter", 0)
		if busMessage != nil {
			splittedBusMessage = strings.Split(string(busMessage.Body), " ")
			responseMessages[receivedCount].monitorid = splittedBusMessage[0]
			responseMessages[receivedCount].statuscode = splittedBusMessage[1]
			receivedCount++

			log.Printf("%d: %s", receivedCount, splittedBusMessage)

			if receivedCount >= maxMessageCountToCollectBeforeInsert {
				log.Println("inserting")
				db, _ = sql.Open("postgres", connStr)
				tx, _ = db.Begin()

				for _, message := range responseMessages {
					tx.Exec(`INSERT INTO "Responses" ("MonitorID", "StatusCode") VALUES ($1, $2);`, message.monitorid, message.statuscode)
				}

				tx.Commit()
				db.Close()
				receivedCount = 0
			}

			sb.DeleteMessage(busMessage)
		}
	}
}
