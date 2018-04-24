package schedule

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/killer-djon/tasks/cron"
)

type Onetime struct {
	row   *model.ScheduleTask
	users []*model.Users
	pub   *publisher.Publisher
	cronJob *cron.Cron
}

type QueueMessage struct {
	UserId       int
	TaskId       int
	MassActionId int
	Text         string
}

type FinalizeMessage struct {
	CoverageCount  int
	PublishCount   int
	UnpublishCount int
	ScheduleId     int
}

// Constructor
func NewOnetime(scheduleModel *model.ScheduleTask, users []*model.Users, cronJob *cron.Cron) *Onetime {
	return &Onetime{
		row: scheduleModel,
		users: users,
		cronJob: cronJob,
	}
}

func (schedule *Onetime) Run(publisherConfig map[string]interface{}) {
	lastRun, _ := time.Parse("2006-01-02 15:04:00", schedule.row.LastRun.Format("2006-01-02 15:04:00"))
	fromDate, _ := time.Parse("2006-01-02 15:04:00", schedule.row.FromDatetime.Format("2006-01-02 15:04:00"))
	//now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

	if( lastRun.Before(fromDate) || lastRun.Equal(fromDate) ) {
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
				fmt.Println("Cronjob must be removed from queue of crons with ID:", schedule.row.Id)
				fmt.Printf("Count of cronjobs=%d", schedule.cronJob.EntriesCount())
				schedule.cronJob.RemoveFunc(schedule.row.Id)

			}
		}
	}
}

func (schedule *Onetime) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
}
