package schedule

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/cron"
)

type Onetime struct {
	row   model.ScheduleTask
	users []*model.Users
	pub   *publisher.Publisher
}

type QueueMessage struct {
	UserId       int
	TaskId       int
	MassActionId int
	Text         string
	Coverage     int
}

type FinalizeMessage struct {
	CoverageCount  int
	PublishCount   int
	UnpublishCount int
	ScheduleId     int
}

func NewOnetime(scheduleModel model.ScheduleTask, pub *publisher.Publisher) *Onetime {
	return &Onetime{
		row: scheduleModel,
		pub: pub,
	}
}

func (schedule *Onetime) Run(publisherConfig map[string]interface{}, database *pgdb.PgDB, cronJob *cron.Cron) map[string]int {

	lastRun, _ := time.Parse("2006-01-02 15:04:00", schedule.row.LastRun.Format("2006-01-02 15:04:00"))
	fromDate, _ := time.Parse("2006-01-02 15:04:00", schedule.row.FromDatetime.Format("2006-01-02 15:04:00"))
	now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

	fmt.Printf("Start cronjob with ID=%d, LastRun=%v, FromDateTime=%v, Now=%v\n", schedule.row.Id, lastRun, fromDate, now)

	var result = make(map[string]int, 2)

	if ( fromDate.Equal(now) ) {
		start := time.Now()
		fmt.Println("Cron job must be paused for work correctly", schedule.row.Id)
		cronJob.PauseFunc(schedule.row.Id)

		users, err := database.GetActiveUsers(schedule.row.Delivery.UserIds, schedule.row.Delivery.Filter)

		if ( err != nil ) {
			fmt.Println("Error to get users by params", err)
			return result
		}

		countPublishing := 0
		countUnPublished := 0

		for _, user := range users {
			q_message := &QueueMessage{
				UserId: user.Id,
				TaskId: schedule.row.Id,
				MassActionId: schedule.row.Delivery.Id,
				Text: schedule.row.Delivery.Text,
				Coverage: len(users),
			}

			fmt.Println("Will be publis data:", q_message)

			message, err := json.Marshal(q_message)
			if err != nil {
				fmt.Println("error:", err)
				countUnPublished++
			}

			channel := schedule.pub
			isPublish, err := channel.Publish(publisherConfig["queue_onetime"].(string), message)

			if err != nil {
				fmt.Println("error on publishing:", err)
				countUnPublished++
			}

			countPublishing++
			fmt.Println("Message will be publish:", isPublish)
		}

		result["countPublishing"] = countPublishing
		result["countUnPublished"] = countUnPublished
		result["lenUsers"] = len(users)

		end := time.Now()
		difference := end.Sub(start)

		fmt.Printf("Time to resolve task: %v\n", difference)
	}

	return result
}

func (schedule *Onetime) SendTransmitStatistic(publisherConfig map[string]interface{}, result map[string]int) bool {
	finalize := &FinalizeMessage{
		CoverageCount:  result["lenUsers"],
		PublishCount: result["countPublishing"],
		UnpublishCount: result["lenUsers"] - result["countPublishing"],
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

	return isPublish
}

func (schedule *Onetime) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
}
