package main

import (
	"log"
	//"time"
	//"bytes"
	"io/ioutil"
	"encoding/json"
	"os"
	"fmt"

	//"github.com/rakanalh/scheduler"
	//"github.com/rakanalh/scheduler/storage"
	"github.com/go-pg/pg"
	"github.com/streadway/amqp"
	"github.com/killer-djon/tasks/pgdb"
	//"github.com/killer-djon/tasks/consumers"
)

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

func readRabbitConsume(amqpURI string, queueName string) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	channel, err := conn.Channel()

	if err != nil {
		log.Fatalf("Open channel: %v", err)
	}
	defer channel.Close()

	q, err := channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Queue.declare: %v", err)
	}

	err = channel.ExchangeDeclare(
		queueName,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	err = channel.QueueBind(q.Name, "", q.Name, false, nil)
	if err != nil {
		log.Fatalf("Queue.bind: %v", err)
	}


	deliveries, err := channel.Consume(
		queueName, //queue name
		"", //consumerTag
		true, //autoAck
		false, //exclusive
		false, //nolocal
		false, //noWait
		nil)        //arguments

	if err != nil {
		log.Fatalf("basic.consume: %v", err)
	}

	// Parse json config file
	amqpString := parseConfig("./config.json")

	db := pgdb.Connect(amqpString["database"])

	defer db.Close()

	forever := make(chan bool)
	go func() {
		for d := range deliveries {
			RunSchedulerTask(d, db)
		}
	}()

	log.Printf(" Waiting for Messages to process. To exit press CTRL+C ")
	<- forever
}

func main() {
	// Parse json config file
	amqpString := parseConfig("./config.json")
	// get format string amqpURI
	amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		amqpString["amqp"]["user"],
		amqpString["amqp"]["password"],
		amqpString["amqp"]["host"],
		amqpString["amqp"]["port"],
		amqpString["amqp"]["vhost"])

	// start consumer
	readRabbitConsume(amqpUri, amqpString["amqp"]["queueName"])
}
