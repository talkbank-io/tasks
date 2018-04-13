package main

import (
	"log"
	//"time"
	//"bytes"
	"io/ioutil"
	"encoding/json"
	"os"
	"fmt"
	"runtime"
	"strings"

	//"github.com/rakanalh/scheduler"
	//"github.com/rakanalh/scheduler/storage"
	"github.com/go-pg/pg"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/tasks/consumers"
)

// Parse json config file
var amqpString = make(map[string]map[string]string)
func init() {
	amqpString := parseConfig("./config.json")
	runtime.GOMAXPROCS(amqpString["maxprocs"])
}

// Read config data.json
func parseConfig(configFile string) map[string]map[string]string {
	config, err := ioutil.ReadFile(configFile)
	var dat = make(map[string]map[string]string)

	if err != nil {
		fmt.Printf("Config file error: %v\n", err)
		os.Exit(1)
	}

	byt := []byte(config)
	error := json.Unmarshal(byt, &dat)
	if error != nil {
		fmt.Printf("Config file error: %v\n", error)
		os.Exit(1)
	}

	return dat
}

// Запуск планировщика задачи
func RunSchedulerTask(d amqp.Delivery, db *pg.DB) {
	body := d.Body
	fmt.Println("TaskWithArgs is executed. message:", string(body))

	var message map[string]interface{}
	json.Unmarshal(body, &message)

	// Результат полученный из базы
	resultSet, err := pgdb.SelectCurrentScheduler(db, message)

	if( err != nil ){
		fmt.Println("Bad response must be requeue")

		d.Reject(true)
	}

	for i, schedule := range resultSet {
		fmt.Printf("Index = %d, Type: %s, Title: %s, Users: %s\n", i, schedule.Type, schedule.Delivery.Title, schedule.Delivery.UserIds)
	}
}


func main() {

	// get format string amqpURI
	/*amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		amqpString["amqp"]["user"],
		amqpString["amqp"]["password"],
		amqpString["amqp"]["host"],
		amqpString["amqp"]["port"],
		amqpString["amqp"]["vhost"])

	// start consumer
	readRabbitConsume(amqpUri, amqpString["amqp"]["queueName"])
	*/
}
