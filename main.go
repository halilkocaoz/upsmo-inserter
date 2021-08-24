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
	maxMessageCount = 250
)

var (
	connstr   string = os.Getenv("AZURE_POSTGRES_CONNSTR")
	namespace string = os.Getenv("SERVICE_BUS_NAMESPACE")
	value     string = os.Getenv("SERVICE_BUS_SHARED_ACCESS_KEY_VALUE")

	messages      [maxMessageCount]string
	receivedCount int = 0

	db *sql.DB
	tx *sql.Tx
)

func main() {
	sb := asbclient.New(asbclient.Topic, namespace, "RootManageSharedAccessKey", value)
	sb.SetSubscription("database-inserter")

	for {
		sbMessage, _ := sb.PeekLockMessage("response-database-inserter", 30)

		if sbMessage != nil {
			message := string(sbMessage.Body)
			messages[receivedCount] = message
			receivedCount++
			log.Printf("%d: %s", receivedCount, message)

			if receivedCount >= maxMessageCount {
				db, _ = sql.Open("postgres", connstr)
				tx, _ = db.Begin()

				for _, m := range messages {
					splitted := strings.Split(m, " ")
					tx.Exec(`INSERT INTO "Responses" ("MonitorID", "StatusCode") VALUES ($1, $2);`, splitted[0], splitted[1])
				}

				tx.Commit()
				db.Close()
				receivedCount = 0
			}

			sb.DeleteMessage(sbMessage)
		}
	}
}
