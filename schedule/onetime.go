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

const (
	TIME_SLEEP_PUBLISH = 3
)

type Onetime struct {
	row   *model.ScheduleTask
	users []*model.Users
	pub   *publisher.Publisher
	db    *pgdb.PgDB
	Hash  string
}

type QueueMessage struct {
	UserId       int
	TaskId       int
	MassActionId int
	Text         string
	Coverage     int
	Hash         string
}

type FinalizeMessage struct {
	CoverageCount  int
	PublishCount   int
	UnpublishCount int
	ScheduleId     int
	ActionType     string
}




func NewOnetime(scheduleModel *model.ScheduleTask, pub *publisher.Publisher, database *pgdb.PgDB) *Onetime {
	return &Onetime{
		row: scheduleModel,
		pub: pub,
		db: database,
	}
}

func (schedule *Onetime) Run(publisherConfig map[string]interface{}, cronJob *cron.Cron) map[string]int {

	fromDate, _ := time.Parse("2006-01-02 15:04:00", schedule.row.FromDatetime.UTC().Format("2006-01-02 15:04:00"))
	now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

	fmt.Printf("Start cronjob with ID=%d, FromDateTime=%v, Now=%v\n", schedule.row.Id, fromDate, now)

	var result = make(map[string]int)

	if( schedule.row.IsActive == true ) {
		fmt.Println("Time is equal like ", (fromDate.Equal(now) || fromDate.Add(time.Minute).Equal(now)))
		if ( fromDate.Equal(now) ) {

			hash, err := schedule.db.SaveHash(schedule.row.Id, schedule.row.Delivery.Id)
			if ( err != nil ) {
				fmt.Println(err)
				cronJob.RemoveFunc(schedule.row.Id)
				return result
			}

			schedule.Hash = hash
			schedule.db.SetIsRunning(schedule.row.Id, true)

			start := time.Now()
			fmt.Println("Cron job must be paused for work correctly", schedule.row.Delivery.Id, schedule.row.Id)
			cronJob.PauseFunc(schedule.row.Id)

			users, err := schedule.db.GetActiveUsers(schedule.row.Delivery.UserIds, schedule.row.Delivery.Filter)

			if ( err != nil ) {
				fmt.Println("Error to get users by params", schedule.row.Delivery.Id, err)
				schedule.db.SetIsRunning(schedule.row.Id, false)
				return result
			}

			if( len(users) < 1 ) {
				fmt.Println("Users to delivery not found", schedule.row.Delivery.Id, users)
				schedule.db.SetIsRunning(schedule.row.Id, false)
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
					Hash: hash,
				}

				fmt.Println("Will be publis data:", schedule.row.Delivery.Id, q_message)

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
				fmt.Println("Message will be publish, and now on:", schedule.row.Delivery.Id, isPublish, countPublishing)
			}

			result["countPublishing"] = countPublishing
			result["countUnPublished"] = countUnPublished
			result["lenUsers"] = len(users)

			end := time.Now()
			difference := end.Sub(start)

			fmt.Printf("Time to resolve task=%d: %v\n", schedule.row.Delivery.Id, difference)
		}
	}else{
		cronJob.RemoveFunc(schedule.row.Id)
	}

	return result
}

func (schedule *Onetime) GetCurrentHash() string {
	return schedule.Hash
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
