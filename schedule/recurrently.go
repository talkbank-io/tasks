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

type Recurrently struct {
	row  *model.ScheduleTask
	pub  *publisher.Publisher
	db   *pgdb.PgDB
	Hash string
}

// Constructor
func NewRecurrently(scheduleModel *model.ScheduleTask, pub *publisher.Publisher, database *pgdb.PgDB) *Recurrently {

	return &Recurrently{
		row: scheduleModel,
		pub: pub,
		db: database,
	}
}

// Запускам задачи по крону от каждой полученной
// записи из schedule_task
func (schedule *Recurrently) Run(publisherConfig map[string]interface{}, cronJob *cron.Cron) map[string]int {
	entry := cronJob.EntryById(schedule.row.Id)

	nextRun, _ := time.Parse("2006-01-02 15:04", schedule.row.NextRun.UTC().Format("2006-01-02 15:04"))
	now, _ := time.Parse("2006-01-02 15:04", time.Now().UTC().Format("2006-01-02 15:04"))

	if ( entry != nil ) {
		fmt.Printf("Recurrently entry to be runned: JobPrev time=%v, JobNext time=%v, Now time=%v, Next runtime=%v\n",
			entry.Prev,
			entry.Next,
			now,
			nextRun)
	}

	var result = make(map[string]int)

	if ( schedule.row.IsActive == true ) {
		if ( nextRun.Equal(now) ) {
			fmt.Println("Cron job must be paused for work correctly", schedule.row.Id)
			cronJob.PauseFunc(schedule.row.Id)

			// Если время следующего запуска совпадает с текущим временем
			// мы запускаем задачу, стопорим cronjob до момент завершения задачи
			// потом заного запускаем до следующего ожидания
			// но при этом надо проверить если nextRun был послденим то мы удаляем job
			hash, err := schedule.db.SaveHash(schedule.row.Id, schedule.row.Delivery.Id)
			if ( err != nil ) {
				fmt.Println("Error on save hash to massAction:", err, schedule.row.Id)
				cronJob.RemoveFunc(schedule.row.Id)
				return result
			}

			schedule.Hash = hash

			schedule.db.SetIsRunning(schedule.row.Id, true)

			start := time.Now()
			users, err := schedule.db.GetActiveUsers(schedule.row.Delivery.UserIds, schedule.row.Delivery.Filter)

			if ( err != nil ) {
				fmt.Println("Error to get users by params", err)
				return result
			}

			if( len(users) < 1 ) {
				fmt.Println("Users to delivery not found", users)
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

				fmt.Println("Will be publis data of the recurrently:", q_message)

				message, err := json.Marshal(q_message)
				if err != nil {
					fmt.Println("error:", err)
					countUnPublished++
				}

				channel := schedule.pub
				isPublish, err := channel.Publish(publisherConfig["queue_recurrently"].(string), message)

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
	} else {
		cronJob.RemoveFunc(schedule.row.Id)
	}

	return result

}

func (schedule *Recurrently) GetCurrentHash() string {
	return schedule.Hash
}

func (schedule *Recurrently) SendTransmitStatistic(publisherConfig map[string]interface{}, result map[string]int) bool {
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

func (schedule *Recurrently) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
}