package main

import (
	"io/ioutil"
	"encoding/json"
	"runtime"
	"fmt"
	"bufio"
	"flag"
	"github.com/talkbank-io/cron"
	"github.com/streadway/amqp"
	"github.com/talkbank-io/tasks/model"
	"github.com/talkbank-io/tasks/pgdb"
	"github.com/talkbank-io/tasks/schedule"
	"github.com/talkbank-io/tasks/publisher"

	"os"
	"time"
)

const (
	CRON_ONETIME_FORMAT = "0 * * * * *" // every minutes
	CRON_EVERY_QUARTER_SECONDS = "*/15 * * * *" // every 15 seconds
	LOG_FILE = "/var/log/tasks/tasks.log"
	NOTIFY_QUEUE = "messengers.notify"
	NOTIFY_GROUP = "error"
	BOT_URL = "https://bot.talkbank.io/admin/massAction/%d/edit"
)

type AlarmMessage struct {
	Text string
	Group string
}



// Status inquires the status of a job, 0: running, 1: paused, -1: not started.
var jobStatus = map[int]string{
	0: "Runned",
	1: "Paused",
	-1: "Not runned",
}

// Parse json config file
var configFile string
var amqpString map[string]interface{}
var configDB = make(map[string]string)

var database *pgdb.PgDB

type CronJob struct {
	w *cron.Cron
}

var cronJob *CronJob
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

	if _, err := os.Stat(LOG_FILE); err == nil {
		_ = os.Remove(LOG_FILE)
	}

	logFile, err := os.OpenFile(LOG_FILE, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0664)
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
		cronJobStatus := cronJob.w.Status(scheduleTaskItem.Id)

		// currentTime, _ := time.Parse("2006-01-02 15:04", time.Now().UTC().Format("2006-01-02 15:04"))
		//currentTime := time.Now().UTC()
		//nextRunDate := scheduleTaskItem.NextRun.UTC()

        currentTime, _ := time.Parse("2006-01-02 15:04", time.Now().UTC().Format("2006-01-02 15:04"))
        nextRunDate, _ := time.Parse("2006-01-02 15:04", scheduleTaskItem.NextRun.UTC().Format("2006-01-02 15:04"))

		// Status inquires the status of a job, 0: running, 1: paused, -1: not started.
		fmt.Printf("Running jobID: %d, actionID=%d, type=%s, and status: %s\n", scheduleTaskItem.Id, scheduleTaskItem.ActionId, scheduleTaskItem.Type, jobStatus[cronJobStatus])
		if ( nextRunDate.Before(currentTime) && scheduleTaskItem.IsRunning == false && (cronJob.w.Status(scheduleTaskItem.Id) == 0)) {
            // alarm:  true ;  false ;  0
			fmt.Println("alarm: ", nextRunDate, currentTime, nextRunDate.Before(currentTime), "; ", scheduleTaskItem.IsRunning, "; ", cronJob.w.Status(scheduleTaskItem.Id))
			notifyAlarm(scheduleTaskItem)
		}else{
			// если задача не запущена
			// или не в паузе тогда создаем задачу и запускаем ее
			if ( (cronJob.w.Status(scheduleTaskItem.Id) == -1 || cronJob.w.Status(scheduleTaskItem.Id) != 1) && nextRunDate.After(currentTime) ) {
				if ( scheduleTaskItem.Type == "onetime" ) {
					fmt.Println("go runOnetime;")
					cronJob.w.AddFunc(CRON_ONETIME_FORMAT, scheduleTaskItem.Id, func() {
						go runOnetime(scheduleTaskItem)
					})

				} else {

					if ( cronJob.w.Status(scheduleTaskItem.Id) == 0 ) {
						entry := cronJob.w.EntryById(scheduleTaskItem.Id)

						fmt.Printf(
							"Recurrently job must be started at: currentTime=%v, nextRun=%v, nextRunJob=%v, is Equal nextrun=%v\n",
							time.Now().UTC(),
							scheduleTaskItem.NextRun.UTC(),
							entry.Next.UTC(),
							nextRunDate.Equal(currentTime))
					}



					// если при запуске задачника мы находим recurrenllty задачу
					// и понимаем что ее надо зупускать, потому что она не была запущена
					// то мы ее запускаем
					// иначе смотрим если дата следующего запуска больше текущей даты
					// тогда запускаем задачник по расписанию в шаблоне
					cronJob.w.AddFunc(CRON_ONETIME_FORMAT, scheduleTaskItem.Id, func() {
						go runRecurrently(scheduleTaskItem)
					})
				}
			} else {
				fmt.Println("not running: ", cronJob.w.Status(scheduleTaskItem.Id), "; ", cronJob.w.Status(scheduleTaskItem.Id), "; ", nextRunDate.After(currentTime), "; ", scheduleTaskItem.NextRun.UTC().Format("2006-01-02 15:04"))
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
		result := pendingTaskSchedule.Run(publisherConfig)

		defer publisherQueue.Close()

		fmt.Println("Pending task will by passed", result)
	}

}

func runRecurrently(scheduleTask model.ScheduleTask) {

	publisherConfig := amqpString["publisher"].(map[string]interface{})
	currentSchedulerTask, err := database.GetSchedulerById(scheduleTask.Id)

	if ( err != nil  ) {
		fmt.Errorf("Cant get schedule by ID: %v", err)
	}

	nextRun, _ := time.Parse("2006-01-02 15:04", currentSchedulerTask.NextRun.UTC().Format("2006-01-02 15:04"))
	now, _ := time.Parse("2006-01-02 15:04", time.Now().UTC().Format("2006-01-02 15:04"))

	if ( currentSchedulerTask.IsActive == true ) {
		if ( nextRun.Equal(now) ) {
			connection, err := getAmqpConnectionChannel()
			if ( err != nil  ) {
				fmt.Errorf("Channel connection is closed: %v", err)
			}

			publisherQueue := publisher.NewPublisher(connection)
			recurrentlyScheduler := schedule.NewRecurrently(currentSchedulerTask, publisherQueue, database)
			result := recurrentlyScheduler.Run(publisherConfig, cronJob.w)

			defer publisherQueue.Close()

			hash := recurrentlyScheduler.GetCurrentHash()

			if ( len(result) < 1 && cronJob.w.Status(scheduleTask.Id) == 1 ) {
				cronJob.w.RemoveFunc(scheduleTask.Id)
			}

			if ( len(result) > 0 ) {
				go func() {
					cronJob.w.AddFunc(CRON_EVERY_QUARTER_SECONDS, (currentSchedulerTask.Id * 1000), func() {
						fmt.Println("Start inner cronjob to check deliveryUsers", scheduleTask.Id)
						go checkDeliveredUsers(publisherConfig, result, currentSchedulerTask.Id, "recurrently", hash)
					})
				}()
			}
		}

	} else {
		cronJob.w.RemoveFunc(currentSchedulerTask.Id)
	}

}

func runOnetime(scheduleTask model.ScheduleTask) {

	publisherConfig := amqpString["publisher"].(map[string]interface{})

	currentSchedulerTask, err := database.GetSchedulerById(scheduleTask.Id)

	if ( err != nil  ) {
		fmt.Errorf("Cant get schedule by ID: %v", err)
	}

	fromDate, _ := time.Parse("2006-01-02 15:04:00", currentSchedulerTask.FromDatetime.UTC().Format("2006-01-02 15:04:00"))
	now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

	if ( currentSchedulerTask.IsActive == true ) {
		if ( fromDate.Equal(now) ) {
			connection, err := getAmqpConnectionChannel()
			if ( err != nil  ) {
				fmt.Errorf("Channel connection is closed: %v", err)
			}

			publisherQueue := publisher.NewPublisher(connection)
			onetimeSchedule := schedule.NewOnetime(currentSchedulerTask, publisherQueue, database)
			result := onetimeSchedule.Run(publisherConfig, cronJob.w)

			defer publisherQueue.Close()

			hash := onetimeSchedule.GetCurrentHash()

			if ( len(result) < 1 && cronJob.w.Status(scheduleTask.Id) == 1 ) {
				cronJob.w.RemoveFunc(scheduleTask.Id)
			}

			if ( len(result) > 0 ) {
				cronJob.w.RemoveFunc(scheduleTask.Id)

				go func() {
					cronJob.w.AddFunc(CRON_EVERY_QUARTER_SECONDS, (scheduleTask.Id * 1000), func() {
						fmt.Println("Start inner cronjob to check deliveryUsers", scheduleTask.Id)
						go checkDeliveredUsers(publisherConfig, result, scheduleTask.Id, "onetime", hash)
					})
				}()
			}
		}
	} else {
		cronJob.w.RemoveFunc(currentSchedulerTask.Id)
	}

}

func notifyAlarm(scheduleTask model.ScheduleTask) {
	newMessage := []AlarmMessage{
		AlarmMessage{
			Text: fmt.Sprintf("MassAction: %d was delayed, and it must be restarted to continue work. %s",
				scheduleTask.ActionId, fmt.Sprintf(BOT_URL, scheduleTask.ActionId)),
			Group: NOTIFY_GROUP,
		},
	}

	alarm_message, err := json.Marshal(newMessage)
	fmt.Println("Message to publich on alarm:", alarm_message)
	if err != nil {
		fmt.Println("error:", err)
	}

	connection, _ := getAmqpConnectionChannel()
	publisherQueue := publisher.NewPublisher(connection)

	_, err = publisherQueue.Publish(NOTIFY_QUEUE, alarm_message)

	if err != nil {
		fmt.Println("error on publishing:", err)
	}

	defer publisherQueue.Close()
}

func checkDeliveredUsers(publisherConfig map[string]interface{}, result map[string]int, scheduleId int, actionType, hash string) {

	countUsersDelivery, _ := database.GetUserDeliveryCountByHash(result, scheduleId, hash)

	fmt.Printf("Count of users_delivery now: %d, coverage: %d\n", countUsersDelivery, result["lenUsers"])

	if ( countUsersDelivery == result["lenUsers"] ) {

		database.SaveStatistic(scheduleId, countUsersDelivery);

		if ( actionType == "recurrently" ) {
			// @TODO we must write statistic information by this schedule
			// and write next runtime of this schedule
			cronJob.w.RemoveFunc(scheduleId)
			fmt.Println("Recurrently job was removed after finish work", scheduleId, ", and must be started on nextTick")
		}

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
		database.SetIsRunning(scheduleId, false)
	}
}

// Get connection to AMQP server
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
