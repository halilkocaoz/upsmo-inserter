package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/michaelbironneau/asbclient"
)

const (
	maxMessageCount = 250
)

var (
	connStr   string = os.Getenv("AZURE_POSTGRES_CONNSTR")
	namespace string = os.Getenv("SERVICE_BUS_NAMESPACE")
	keyValue  string = os.Getenv("SERVICE_BUS_SHARED_ACCESS_KEY_VALUE")

	messages      [maxMessageCount]string
	receivedCount int = 0

	insertingBeginAt int64
	insertingEndAt   int64

	db *sql.DB
	tx *sql.Tx
)

func main() {
	fmt.Printf("%s\n%s\n%s\n", connStr, namespace, keyValue)
	sb := asbclient.New(asbclient.Topic, namespace, "RootManageSharedAccessKey", keyValue)
	sb.SetSubscription("database-inserter")

	for {
		sbMessage, _ := sb.PeekLockMessage("response-database-inserter", 0)
		if sbMessage != nil {
			message := string(sbMessage.Body)
			messages[receivedCount] = message
			receivedCount++
			log.Printf("%d: %s", receivedCount, message)

			if receivedCount >= maxMessageCount {
				log.Println("inserting")
				insertingBeginAt = time.Now().Unix()
				db, _ = sql.Open("postgres", connStr)
				tx, _ = db.Begin()

				for _, m := range messages {
					splitted := strings.Split(m, " ")
					tx.Exec(`INSERT INTO "Responses" ("MonitorID", "StatusCode") VALUES ($1, $2);`, splitted[0], splitted[1])
				}

				tx.Commit()
				db.Close()
				insertingEndAt = time.Now().Unix()
				log.Printf("elapsed sec: %d", insertingEndAt-insertingBeginAt)
				receivedCount = 0
			}
			sb.DeleteMessage(sbMessage)
		}
	}
}
