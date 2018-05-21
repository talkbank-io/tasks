package schedule

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/killer-djon/tasks/pgdb"
)

type Pending struct {
	rows  []model.PendingTask
	users []*model.Users
	pub   *publisher.Publisher
	db    *pgdb.PgDB
}

func NewPending(pendingTasks []model.PendingTask, pub *publisher.Publisher, database *pgdb.PgDB) *Pending {
	return &Pending{
		rows: pendingTasks,
		pub: pub,
		db: database,
	}
}

func (pending *Pending) Run(publisherConfig map[string]interface{}) {
	fmt.Println("All records pending:", pending.rows[0].ScheduleTask.Id, pending.rows[0].Delivery.Id)

	start := time.Now()
	for _, pendingItem := range pending.rows {
		hash, err := pending.db.SaveHash(pendingItem.ScheduleTask.Id, pendingItem.Delivery.Id)
		if ( err != nil ) {
			fmt.Println("Error on set hash:", err)
		}

		q_message := &QueueMessage{
			UserId: pendingItem.UserId,
			TaskId: pendingItem.ScheduleTask.Id,
			MassActionId: pendingItem.Delivery.Id,
			Text: pendingItem.Delivery.Text,
			Coverage: 1,
			Hash: hash,
		}

		message, err := json.Marshal(q_message)
		if err != nil {
			fmt.Println("error on Marshall message to queue:", err)
		}

		channel := pending.pub
		isPublish, err := channel.Publish(publisherConfig["queue_pending"].(string), message)

		if err != nil {
			fmt.Println("error on publishing:", err)
		}

		fmt.Println("Message will be publish:", isPublish)
	}

	end := time.Now()
	difference := end.Sub(start)

	fmt.Printf("Time to resolve task: %v\n", difference)
}