package main

import (
	"log"
	"io/ioutil"
	"encoding/json"
	"runtime"
	"github.com/killer-djon/cron"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/tasks/consumers"
	"github.com/killer-djon/tasks/schedule"
	"github.com/killer-djon/tasks/publisher"
)

const (
	CRON_ONETIME_FORMAT = "1 * * * * *" // every minutes
	CRON_RECURRENTLY_FORMAT = ""
)

// Parse json config file
var amqpString, _ = parseConfig("./config.json")
var configDB = make(map[string]string)

var database *pgdb.PgDB

type CronJob struct {
	w *cron.Cron
}

var cronJob *CronJob
var conn *consumers.Consumer

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

	go StartOnetimeScheduler()
	StartConsumer()
}

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
		go StartOnetimeScheduler()
	}
}

// Запуск планировщика по расписанию
func StartRecurrentlyScheduler(scheduleTask *model.ScheduleTask) {

}

// Запуск разовой задачи по отправке
func StartOnetimeScheduler() {

	resultSet, err := database.SelectCurrentScheduler()

	if( err != nil ){
		fmt.Println("Error to get data from Db", err)
	}

	for _, scheduleItem := range resultSet {
		if ( scheduleItem.Type == "onetime" ){
			scheduleTaskItem := scheduleItem

			cronJob.w.AddFunc(CRON_ONETIME_FORMAT, scheduleTaskItem.Id, func() {
				go runOnetime(scheduleTaskItem)
			})

		}else {
			//go StartRecurrentlyScheduler(&scheduleItem)
		}
	}

}

func runOnetime(scheduleTask model.ScheduleTask) {
	publisherConfig := amqpString["publisher"].(map[string]interface{})
	publisherQueue := publisher.NewPublisher(conn.GetConnection())
	onetimeSchedule := schedule.NewOnetime(scheduleTask, publisherQueue, database)
	result := onetimeSchedule.Run(publisherConfig, cronJob.w)

	if( len(result) > 0 ){
		publish := onetimeSchedule.SendTransmitStatistic(publisherConfig, result)
		if( publish == true ){
			fmt.Printf(
				"Cron job with ID=%d will be running succefull, Coverage count=%d, published count=%d, unPublished count=%d\n",
				scheduleTask.Id,
				result["lenUsers"],
				result["countPublishing"],
				result["countUnPublished"])
			cronJob.w.RemoveFunc(scheduleTask.Id)
		}
	}
}