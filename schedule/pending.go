package schedule

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/talkbank-io/tasks/model"
	"github.com/talkbank-io/tasks/publisher"
	"github.com/talkbank-io/tasks/pgdb"
)


type Pending struct {
	rows  []model.PendingTask
	users []*model.Users
	pub   *publisher.Publisher
	db    *pgdb.PgDB
	Hash string
}

func NewPending(pendingTasks []model.PendingTask, pub *publisher.Publisher, database *pgdb.PgDB) *Pending {
	return &Pending{
		rows: pendingTasks,
		pub: pub,
		db: database,
	}
}

func (pending *Pending) Run(publisherConfig map[string]interface{}) map[string]int {
	fmt.Println("All records pending:", pending.rows[0].ScheduleTask.Id, pending.rows[0].Delivery.Id)

	var result = make(map[string]int)

	countPublishing := 0
	countUnPublished := 0

	start := time.Now()
	for _, pendingItem := range pending.rows {
		hash, err := pending.db.SaveHash(pendingItem.ScheduleTask.Id, pendingItem.Delivery.Id)
		if ( err != nil ) {
			fmt.Println("Error on set hash:", err)
		}

		pending.Hash = hash

		q_message := &PendingMessage{
			UserId: pendingItem.UserId,
			TaskId: pendingItem.ScheduleTask.Id,
			MassActionId: pendingItem.Delivery.Id,
			Text: pendingItem.Delivery.Text,
			Coverage: len(pending.rows),
			Hash: hash,
			PendingId: pendingItem.Id,
		}

		message, err := json.Marshal(q_message)
		if err != nil {
			fmt.Println("error on Marshall message to queue:", err)
			return result
		}

		channel := pending.pub
		isPublish, err := channel.Publish(publisherConfig["queue_pending"].(string), message)

		if err != nil {
			fmt.Println("error on publishing:", err)
			countUnPublished++
		}

		pending.db.IncSentDelivery(pendingItem, pendingItem.Delivery.Id, 1)

		countPublishing++
		fmt.Println("Message will be publish:", isPublish, countPublishing)
	}

	result["countPublishing"] = countPublishing
	result["countUnPublished"] = countUnPublished
	result["lenUsers"] = len(pending.rows)

	end := time.Now()
	difference := end.Sub(start)

	fmt.Printf("Time to resolve task: %v\n", difference)

	return result
}

func (pending *Pending) GetCurrentHash() string {
	return pending.Hash
}