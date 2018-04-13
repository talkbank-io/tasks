package main

import (
	"log"
	//"strconv"
	//"time"
	//"bytes"
	"io/ioutil"
	"encoding/json"
	//"os"

	"fmt"
	//"strings"

	//"github.com/rakanalh/scheduler"
	//"github.com/rakanalh/scheduler/storage"
	"github.com/go-pg/pg"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/tasks/consumers"
)

// Parse json config file
var amqpString, _ = parseConfig("./config.json")
var configDB = make(map[string]string)
var db *pg.DB

func init()  {
	config := amqpString["database"].(map[string]interface{})
	for key, value := range config {
		configDB[key] = value.(string)
	}

	db = pgdb.Connect(configDB)
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

	amqpConfig := amqpString["amqp"].(map[string]interface{})

	amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		amqpConfig["user"].(string),
		amqpConfig["password"].(string),
		amqpConfig["host"].(string),
		amqpConfig["port"].(string),
		amqpConfig["vhost"].(string))

	conn := consumers.NewConsumer(
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

	fmt.Printf("Thrad nubner %d", threads)
	conn.Handle(deliveries, handler, threads, amqpConfig["queueName"].(string), "")

}


func handler(deliveries <-chan amqp.Delivery) {

	for d := range deliveries {
		var message map[string]interface{}
		json.Unmarshal(d.Body, &message)

		fmt.Println("Got message from queue:", message)
		RunSchedulerTask(d, db)
	}

}


// Запуск планировщика задачи
func RunSchedulerTask(d amqp.Delivery, db *pg.DB) {
	body := d.Body
	fmt.Println("TaskWithArgs is executed. message:", string(body))

	var message map[string]interface{}
	json.Unmarshal(body, &message)

	// Результат полученный из базы
	resultSet, err := pgdb.SelectCurrentScheduler(db)

	if( err != nil ){
		fmt.Println("Bad response must be requeue")
		d.Ack(true)
	}

	for _, schedule := range resultSet {
		/*fmt.Printf("Index = %d, Type: %s, Title: %s, Users: %s, Template: %s\n", i,
			schedule.Type,
			schedule.Delivery.Title,
			schedule.Delivery.UserIds,
			schedule.Template)
		*/
		if ( schedule.Type == "onetime" ){
			StartOnetimeScheduler(schedule.Id, schedule.Template)
		}else {
			StartRecurrentlyScheduler(schedule.Id, schedule.Template)
		}
	}

	d.Ack(false)
}

// Запуск планировщика по расписанию
func StartRecurrentlyScheduler(scheduleID int64, template string) {
	fmt.Printf("Start recurrently scheduler at Index: %d, with template: %s\n", scheduleID, template)
}

func StartOnetimeScheduler(scheduleID int64, template string) {
	fmt.Printf("Start onetime scheduler at Index: %d, with template: %s\n", scheduleID, template)
}