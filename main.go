package main

import (
	"log"
	"os"

	"github.com/michaelbironneau/asbclient"
)

const (
	maxMessageCount = 250
)

var (
	namespace string = os.Getenv("SERVICE_BUS_NAMESPACE")
	value     string = os.Getenv("SERVICE_BUS_SHARED_ACCESS_KEY_VALUE")

	messages      [maxMessageCount]string
	receivedCount int = 0
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
				receivedCount = 0
			}

			sb.DeleteMessage(sbMessage)
		}
	}
}
