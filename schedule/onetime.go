package schedule

import (
	"fmt"
	//"strconv"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	//"github.com/elgs/cron"
)

type Onetime struct {
	row   model.ScheduleTask
	users []*model.Users
	pub   *publisher.Publisher
}

type QueueMessage struct {
	UserId       int64
	TaskId       int64
	MassActionId int64
	Text         string
}

type FinalizeMessage struct {
	CoverageCount  int
	PublishCount   int
	UnpublishCount int
	ScheduleId     int64
}

// Constructor
func NewOnetime(scheduleModel model.ScheduleTask, users []*model.Users) *Onetime {
	return &Onetime{
		row: scheduleModel,
		users: users,
	}
}

func (schedule *Onetime) Run(publisherConfig map[string]interface{}) {

	countPublishing := 0
	for _, user := range schedule.users {
		q_message := &QueueMessage{
			UserId: user.Id,
			TaskId: schedule.row.Id,
			MassActionId: schedule.row.Delivery.Id,
			Text: schedule.row.Delivery.Text,
		}

		fmt.Println("Will be publis data:", q_message)

		message, err := json.Marshal(q_message)
		if err != nil {
			fmt.Println("error:", err)
		}

		channel := schedule.pub
		isPublish, err := channel.Publish(publisherConfig["queue_onetime"].(string), message)

		if err != nil {
			fmt.Println("error on publishing:", err)
		}

		countPublishing++

		fmt.Println("Message will be publish:", isPublish)

	}

	if ( countPublishing == len(schedule.users) ){
		fmt.Printf("Items will be published countPublished=%d, lenUsers=%d", countPublishing, len(schedule.users))
	}

}

func (schedule *Onetime) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
}
