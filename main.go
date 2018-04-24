package main

import (
	"log"
	"io/ioutil"
	"encoding/json"
	"runtime"
	//"time"
	//"strings"
	//"strconv"
	//"github.com/elgs/cron"

	"github.com/killer-djon/tasks/cron"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/tasks/consumers"
	"github.com/killer-djon/tasks/schedule"
	"github.com/killer-djon/tasks/publisher"
)

// Parse json config file
var amqpString, _ = parseConfig("./config.json")
var configDB = make(map[string]string)

var database pgdb.PgDB

type CronJob struct {
	w *cron.Cron
}

var cronJob *CronJob

func init()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config := amqpString["database"].(map[string]interface{})
	for key, value := range config {
		configDB[key] = value.(string)
	}

	database = pgdb.NewPgDB(configDB)
	cronJob = &CronJob{
		w: cron.New(),
	}
	cronJob.w.Start()
}

// Read config data.json
func parseConfig(configFile string) (map[string]interface{}, error) {
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

	StartOnetimeScheduler()
	StartConsumer()
}

var conn *consumers.Consumer

func StartConsumer() {
	amqpConfig := amqpString["amqp"].(map[string]interface{})
	amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		amqpConfig["user"].(string),
		amqpConfig["password"].(string),
		amqpConfig["host"].(string),
		amqpConfig["port"].(string),
		amqpConfig["vhost"].(string))

	conn = consumers.NewConsumer(
		"",
		amqpUri,
		amqpConfig["queueName"].(string),
		amqpConfig["exchangeType"].(string),
		amqpConfig["queueName"].(string),
	)

	if err := conn.Connect(); err != nil {
		log.Printf("Error: %v", err)
	}

	deliveries, err := conn.AnnounceQueue(amqpConfig["queueName"].(string), "")
	if err != nil {
		log.Printf("Error when calling AnnounceQueue(): %v", err.Error())
	}

	var threads int
	threadsConfig := amqpString["threads"].(float64)
	threads = int(threadsConfig)

	fmt.Printf("Thread number %d\n", threads)
	conn.Handle(deliveries, handler, threads, amqpConfig["queueName"].(string), "")
}


func handler(deliveries <-chan amqp.Delivery) {

	for d := range deliveries {
		d.Ack(false)
		StartOnetimeScheduler()
	}
}


// Запуск планировщика задачи
func RunSchedulerTask(d amqp.Delivery) {

	body := d.Body
	fmt.Println("TaskWithArgs is executed. message:", string(body))

	var message map[string]interface{}
	json.Unmarshal(body, &message)

	fmt.Printf("Count of entries=%d, and Status running=%t\n", cronJob.w.EntriesCount(), cronJob.w.IsRunning())

	StartOnetimeScheduler()

	d.Ack(false)
}


// Запуск планировщика по расписанию
func StartRecurrentlyScheduler(scheduleTask *model.ScheduleTask) {
	//fmt.Printf("Start recurrently scheduler at Schedule ID: %d, with template: %s\n", scheduleID, template)
	//recurrentlySchedule := schedule.NewRecurrently(scheduleTask)
	//recurrentlySchedule.Print()

	/*users, err := database.GetActiveUsers()

	if( err != nil ){
		fmt.Println("Error to get data users", err)
	}

	for _, userMessenger := range users {
		fmt.Println(userMessenger.Id)
	}*/
	fmt.Println(scheduleTask.Delivery.UserIds, scheduleTask.Delivery.Filter)

	//835, 817, 829, 832, 796, 802,  808
}

func StartOnetimeScheduler() {

	resultSet, err := database.SelectCurrentScheduler()

	if( err != nil ){
		fmt.Println("Error to get data from Db", err)
	}

	for _, scheduleItem := range resultSet {
		if ( scheduleItem.Type == "onetime" ){
			scheduleTaskItem := scheduleItem
			cronJob.w.AddFunc("*/5 * * * * *", scheduleTaskItem.Id, func() {
				go runOnetime(scheduleTaskItem.Id)
			})

		}else {
			//go StartRecurrentlyScheduler(&scheduleItem)
		}
	}
	/*
	scheduleTask, _ := database.GetSchedulerById(scheduleTaskId)
	publisherConfig := amqpString["publisher"].(map[string]interface{})

	lastRun, _ := time.Parse("2006-01-02 15:04:00", scheduleTask.LastRun.Format("2006-01-02 15:04:00"))
	fromDate, _ := time.Parse("2006-01-02 15:04:00", scheduleTask.FromDatetime.Format("2006-01-02 15:04:00"))
	now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

	fmt.Printf("Cronjob will be running with Id=%d, Lastrundate=%v, Fromdatetime=%v, Now=%v\n ", cronJob.w.EntryById(scheduleTask.Id), lastRun, fromDate, now)

	if ( lastRun.Before(fromDate) ){

		fmt.Println("Lastrun date is before fromDatetime", lastRun, fromDate)
		users, err := database.GetActiveUsers(scheduleTask.Delivery.UserIds, scheduleTask.Delivery.Filter)

		if ( err != nil ) {
			fmt.Println("Error to get users by params", err)
		}

		publisherQueue := publisher.NewPublisher(conn.GetConnection())

		onetimeSchedule := schedule.NewOnetime(scheduleTask, users, cronJob.w)
		onetimeSchedule.SetAmqp(publisherQueue)
		onetimeSchedule.Run(publisherConfig)
	}
	*/

}

func runOnetime(scheduleTaskId int) {
	scheduleTask, _ := database.GetSchedulerById(scheduleTaskId)
	if ( scheduleTask == nil ) {
		cronJob.w.RemoveFunc(scheduleTaskId)
	}

	publisherConfig := amqpString["publisher"].(map[string]interface{})

	users, err := database.GetActiveUsers(scheduleTask.Delivery.UserIds, scheduleTask.Delivery.Filter)

	if ( err != nil ) {
		fmt.Println("Error to get users by params", err)
	}

	publisherQueue := publisher.NewPublisher(conn.GetConnection())

	onetimeSchedule := schedule.NewOnetime(scheduleTask, users, cronJob.w)
	onetimeSchedule.SetAmqp(publisherQueue)
	onetimeSchedule.Run(publisherConfig)
}