package main

import (
	"log"
	"io/ioutil"
	"encoding/json"
	"runtime"
	"strings"
	"strconv"
	"github.com/elgs/cron"

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

func init()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config := amqpString["database"].(map[string]interface{})
	for key, value := range config {
		configDB[key] = value.(string)
	}

	database = pgdb.NewPgDB(configDB)


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

	go func(){
		// Результат полученный из базы
		resultSet, err := database.SelectCurrentScheduler()


		if( err != nil ){
			fmt.Println("Error to get data from Db", err)
		}

		cronJob := cron.New()
		cronJob.Start()

		for i, scheduleItem := range resultSet {
			if ( scheduleItem.Type == "onetime" ){

				jobNumber := i+1
				jobStatus := cronJob.Status(jobNumber)
				fmt.Println("Job Status: ", jobStatus)

				if( jobStatus == -1 ) {
					fmt.Println("SChedule item to start:", scheduleItem.Id)
					scheduleTaskItem := scheduleItem
					cronJob.AddFunc("*/3 * * * * *", func(){
						fmt.Println("add job function:", scheduleTaskItem.Id, jobNumber)
						StartOnetimeScheduler(scheduleTaskItem, jobNumber, cronJob)
					})

				}
			}else {
				//go StartRecurrentlyScheduler(&scheduleItem)
			}
		}
	}()

	runConsumer()
}

var conn *consumers.Consumer

func runConsumer() {
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
		var message map[string]interface{}
		json.Unmarshal(d.Body, &message)

		fmt.Println("Got message from queue:", message)
		RunSchedulerTask(d)
	}
}


// Запуск планировщика задачи
func RunSchedulerTask(d amqp.Delivery) {

	body := d.Body
	fmt.Println("TaskWithArgs is executed. message:", string(body))

	var message map[string]interface{}
	json.Unmarshal(body, &message)

	// Результат полученный из базы
	resultSet, err := database.SelectCurrentScheduler()

	if( err != nil ){
		fmt.Println("Bad response must be requeue")
		d.Ack(true)
	}

	cronJob := cron.New()
	cronJob.Start()

	for i, scheduleItem := range resultSet {
		if ( scheduleItem.Type == "onetime" ){
			jobNumber := i+1
			jobStatus := cronJob.Status(jobNumber)
			fmt.Println("Job Status: ", jobStatus)

			if( jobStatus == -1 ) {
				fmt.Println("SChedule item to start:", scheduleItem.Id)
				scheduleTaskItem := scheduleItem
				cronJob.AddFunc("*/3 * * * * *", func(){
					go StartOnetimeScheduler(scheduleTaskItem, jobNumber, cronJob)
				})

			}
		}else {
			//go StartRecurrentlyScheduler(&scheduleItem)
		}
	}

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

func StartOnetimeScheduler(scheduleTask model.ScheduleTask, jobNumber int, cronJob *cron.Cron) {

	lastRun := scheduleTask.LastRun.UTC()
	fromDate := scheduleTask.FromDatetime.UTC()

	fmt.Println("Start on schedule id:", scheduleTask.Id)

	if ( lastRun.Before(fromDate) ){

		userStringsIds := strings.Split(strings.Trim(scheduleTask.Delivery.UserIds, " "), ",")
		userIds := make([]int, len(userStringsIds))
		for i, userId := range userStringsIds {
			trimStringUserId := strings.Trim(userId, " ")
			userIds[i], _ = strconv.Atoi(trimStringUserId)
		}

		users, err := database.GetActiveUsers(userIds, scheduleTask.Delivery.Filter)

		if ( err != nil ) {
			fmt.Println("Error to get users by params", err)
		}

		publisherQueue := publisher.NewPublisher(conn.GetConnection())

		onetimeSchedule := schedule.NewOnetime(scheduleTask, users)
		onetimeSchedule.SetAmqp(publisherQueue)
		onetimeSchedule.Run(cronJob, jobNumber)
	}else{
		cronJob.RemoveFunc(jobNumber)
	}
}
