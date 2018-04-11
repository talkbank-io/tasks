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
	"github.com/streadway/amqp"
)

func TaskWithArgs(message string) {
	fmt.Println("TaskWithArgs is executed. message:", message)
	//fmt.Printf("got message as String: %s\n", message)
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

func readRabbitConsume(amqpURI string, queueName string) {
	conn, _ := amqp.Dial(amqpURI)
	channel, _ := conn.Channel()

	defer channel.Close()
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

	forever := make(chan bool)
	go func() {
		for d := range deliveries {
			TaskWithArgs(string(d.Body))
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
