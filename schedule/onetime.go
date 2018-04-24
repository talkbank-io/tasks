package schedule

import (
	"fmt"
	//"strconv"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/elgs/cron"
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

func (schedule *Onetime) Run(cronJob *cron.Cron, jobNumber int, publisherConfig map[string]interface{}) {
	fmt.Println(cronJob.Entries())
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

	if ( countPublishing == len(schedule.users) ) {

		finalize := &FinalizeMessage{
			CoverageCount:  len(schedule.users),
			PublishCount: countPublishing,
			UnpublishCount: len(schedule.users) - countPublishing,
			ScheduleId: schedule.row.Id,
		}

		finalize_message, err := json.Marshal(finalize)
		if err != nil {
			fmt.Println("error:", err)
		}

		channel := schedule.pub
		isPublish, err := channel.Publish(publisherConfig["queue_statistic"].(string), finalize_message)

		if err != nil {
			fmt.Println("error on publishing:", err)
		}

		if ( isPublish == true ) {
			fmt.Println("Publish statistic info to queue will send", finalize)
		}

		cronJob.RemoveFunc(jobNumber)
		fmt.Println("Cron job will be removed:", jobNumber)
	}

}

func (schedule *Onetime) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
}
