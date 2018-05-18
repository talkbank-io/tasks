package main

import (
	"io/ioutil"
	"encoding/json"
	"runtime"
	"fmt"
	"bufio"
	"flag"
	"github.com/killer-djon/cron"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/tasks/consumers"
	"github.com/killer-djon/tasks/schedule"
	"github.com/killer-djon/tasks/publisher"
	"os"
	"time"
)

const (
	CRON_ONETIME_FORMAT = "0 * * * * *" // every minutes
	CRON_EVERY_QUARTER_SECONDS = "*/15 * * * *" // every 15 seconds
)

// Parse json config file
var configFile string
var amqpString map[string]interface{}
var configDB = make(map[string]string)

var database *pgdb.PgDB

type CronJob struct {
	w *cron.Cron
}

var cronJob *CronJob
var conn *consumers.Consumer
var writer *bufio.Writer

func init() {
	flag.StringVar(&configFile, "configFile", "./config.json", "Get config file with params")
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// Read config data.json
func parseConfig() (map[string]interface{}, error) {
	config, err := ioutil.ReadFile(configFile)
	var dat = make(map[string]interface{})

	if err != nil {
		fmt.Printf("Config file error: %v\n", err)
		return nil, err
	}

	byt := []byte(config)
	error := json.Unmarshal(byt, &dat)

	if error != nil {
		fmt.Printf("Config file error: %v\n", error)
		return nil, error
	}

	return dat, nil
}

func main() {
	flag.Parse()
	fmt.Println(configFile)

	amqpString, _ = parseConfig()

	config := amqpString["database"].(map[string]interface{})
	for key, value := range config {
		configDB[key] = value.(string)
	}

	database = pgdb.NewPgDB(configDB)
	cronJob = &CronJob{
		w: cron.New(),
	}
	cronJob.w.Start()
	cronJob.w.Reset()

	for _, cj := range cronJob.w.Entries() {
		fmt.Println("Runned cronjos by Id:", cj)
	}

	logFile, err := os.OpenFile("/var/log/tasks/tasks.log", os.O_RDWR | os.O_APPEND | os.O_CREATE, 0664)
	if ( err != nil ) {
		fmt.Printf("ERror on create/open log file=%v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()
	writer = bufio.NewWriter(logFile)

	cronJob.w.AddFunc(CRON_ONETIME_FORMAT, 0, func() {
		pendings, err := database.SelectPendingTasks()
		if ( err != nil ) {
			fmt.Fprintf(writer, "Error to get data from PendingTask=%v\n", err)
			writer.Flush()
		}

		go runPendingTask(pendings)
		go StartSchedulersJob()
	})

	go StartSchedulersJob()

	select {

	}

}


// Запуск разовой задачи по отправке
func StartSchedulersJob() {
	resultSet, err := database.SelectCurrentScheduler()

	if ( err != nil ) {
		fmt.Println("Error to get data from Db", err)
		fmt.Fprintf(writer, "Error to get data from Db=%v\n", err)
		writer.Flush()
	}

	fmt.Printf("Count running jobs: %d\n", cronJob.w.EntriesCount())
	fmt.Println("Len of the records:", len(resultSet))

	fmt.Fprintf(writer, "Len of the records=%d\n", len(resultSet))
	writer.Flush()

	for _, scheduleItem := range resultSet {
		scheduleTaskItem := scheduleItem

		fmt.Printf("Running jobID: %d, and status: %d\n", scheduleTaskItem.Id, cronJob.w.Status(scheduleTaskItem.Id))
		if ( cronJob.w.Status(scheduleTaskItem.Id) != 1 || cronJob.w.Status(scheduleTaskItem.Id) == -1 ) {
			if ( scheduleTaskItem.Type == "onetime" ) {
				cronJob.w.AddFunc(CRON_ONETIME_FORMAT, scheduleTaskItem.Id, func() {
					go runOnetime(scheduleTaskItem)
				})

			} else {

				var resultTemplate = make(map[string]string)
				json.Unmarshal([]byte(scheduleTaskItem.Template), &resultTemplate)

				cronTemplate := fmt.Sprintf("0 %s %s %s %s %s",
					resultTemplate["minute"],
					resultTemplate["hour"],
					resultTemplate["day"],
					resultTemplate["month"],
					resultTemplate["weekday"],
				)
				fmt.Println("Cronjob recurrently template", cronTemplate, scheduleTaskItem.Id)

				cronJob.w.AddFunc(cronTemplate, scheduleTaskItem.Id, func() {
					entry := cronJob.w.EntryById(scheduleTaskItem.Id)
					fmt.Printf(
						"Recurrently job must be started at: currentTime=%v, nextRun=%v, nextRunJob=%v\n",
						time.Now().UTC(),
						scheduleTaskItem.NextRun,
						entry.Next.UTC())
					go runRecurrently(scheduleTaskItem)
				})
			}
		}
	}

}

func runPendingTask(pendingTasks []model.PendingTask) {
	fmt.Println("Length of pending task records:", len(pendingTasks))
	fmt.Fprintf(writer, "Length of pending task records: %d", len(pendingTasks))

	if ( len(pendingTasks) > 0 ) {
		publisherConfig := amqpString["publisher"].(map[string]interface{})
		connection, err := getAmqpConnectionChannel()
		if ( err != nil  ) {
			fmt.Errorf("Channel connection is closed: %v", err)
		}
		publisherQueue := publisher.NewPublisher(connection)
		pendingTaskSchedule := schedule.NewPending(pendingTasks, publisherQueue, database)
		pendingTaskSchedule.Run(publisherConfig)
	}

}

func runRecurrently(scheduleTask model.ScheduleTask) {
	if( scheduleTask.IsActive == true ) {
		publisherConfig := amqpString["publisher"].(map[string]interface{})
		connection, err := getAmqpConnectionChannel()
		if ( err != nil  ) {
			fmt.Errorf("Channel connection is closed: %v", err)
		}
		currentSchedulerTask, err := database.GetSchedulerById(scheduleTask.Id)
		fmt.Println(currentSchedulerTask)
		publisherQueue := publisher.NewPublisher(connection)
		recurrentlyScheduler := schedule.NewRecurrently(currentSchedulerTask, publisherQueue, database)
		result := recurrentlyScheduler.Run(publisherConfig, cronJob.w)

		if ( len(result) > 0 ) {

			go func() {
				cronJob.w.AddFunc(CRON_EVERY_QUARTER_SECONDS, (scheduleTask.Id * 1000), func() {
					fmt.Println("Start inner cronjob to check deliveryUsers", scheduleTask.Id)
					go checkDeliveredUsers(publisherConfig, result, scheduleTask.Id, "recurrently")
				})
			}()
		}
	}else {
		cronJob.w.RemoveFunc(scheduleTask.Id)
	}
}

func runOnetime(scheduleTask model.ScheduleTask) {
	if( scheduleTask.IsActive == true ) {
		publisherConfig := amqpString["publisher"].(map[string]interface{})
		connection, err := getAmqpConnectionChannel()
		if ( err != nil  ) {
			fmt.Errorf("Channel connection is closed: %v", err)
		}

		currentSchedulerTask, err := database.GetSchedulerById(scheduleTask.Id)
		fmt.Println(currentSchedulerTask)

		if ( err != nil  ) {
			fmt.Errorf("Cant get schedule by ID: %v", err)
		}

		publisherQueue := publisher.NewPublisher(connection)
		onetimeSchedule := schedule.NewOnetime(currentSchedulerTask, publisherQueue, database)
		result := onetimeSchedule.Run(publisherConfig, cronJob.w)

		if ( len(result) > 0 ) {
			cronJob.w.RemoveFunc(scheduleTask.Id)
			go func() {
				cronJob.w.AddFunc(CRON_EVERY_QUARTER_SECONDS, (scheduleTask.Id * 1000), func() {
					fmt.Println("Start inner cronjob to check deliveryUsers", scheduleTask.Id)
					go checkDeliveredUsers(publisherConfig, result, scheduleTask.Id, "onetime")
				})
			}()
		}
	}else {
		cronJob.w.RemoveFunc(scheduleTask.Id)
	}
}

func checkDeliveredUsers(publisherConfig map[string]interface{}, result map[string]int, scheduleId int, actionTipe string) {

	countUsersDelivery, _ := database.GetUserDeliveryCountByHash(result, scheduleId)

	fmt.Printf("Count of users_delivery now: %d, coverage: %d", countUsersDelivery, result["lenUsers"])
	if ( countUsersDelivery == result["lenUsers"] ) {
		finalize := &schedule.FinalizeMessage{
			CoverageCount:  result["lenUsers"],
			PublishCount: result["countPublishing"],
			UnpublishCount: result["lenUsers"] - result["countPublishing"],
			ScheduleId: scheduleId,
			ActionType: actionTipe,
		}

		finalize_message, err := json.Marshal(finalize)
		if err != nil {
			fmt.Println("error:", err)
		}
		connection, _ := getAmqpConnectionChannel()
		publisherQueue := publisher.NewPublisher(connection)

		isPublish, err := publisherQueue.Publish(publisherConfig["queue_statistic"].(string), finalize_message)

		if err != nil {
			fmt.Println("error on publishing:", err)
		}

		if ( isPublish == true ) {
			fmt.Printf(
				"Cron job with ID=%d will be running succefull, Coverage count=%d, published count=%d, unPublished count=%d\n",
				scheduleId,
				result["lenUsers"],
				result["countPublishing"],
				result["countUnPublished"])

			fmt.Fprintf(writer, "Cron job with ID=%d will be running succefull, Coverage count=%d, published count=%d, unPublished count=%d\n",
				scheduleId,
				result["lenUsers"],
				result["countPublishing"],
				result["countUnPublished"])

			writer.Flush()
			cronJob.w.RemoveFunc(scheduleId * 1000)

			currentScheduler, _ := database.GetSchedulerById(scheduleId)
			database.SetIsRunning(scheduleId, false)

			if ( currentScheduler.Type == "onetime" ) {
				cronJob.w.RemoveFunc(scheduleId)
			} else {
				if ( currentScheduler.IsActive == false ) {
					cronJob.w.RemoveFunc(scheduleId)
				} else {
					cronJob.w.ResumeFunc(scheduleId)
				}
			}
		}
	}
}

func getAmqpConnectionChannel() (*amqp.Connection, error) {
	amqpConfig := amqpString["amqp"].(map[string]interface{})
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		amqpConfig["user"].(string),
		amqpConfig["password"].(string),
		amqpConfig["host"].(string),
		amqpConfig["port"].(string),
		amqpConfig["vhost"].(string))

	connection, err := amqp.Dial(amqpURI)

	if err != nil {
		fmt.Errorf("Dial: %s", err)
		return nil, err
	}

	return connection, nil
}
