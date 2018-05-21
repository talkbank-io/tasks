package publisher

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
)

type Publisher struct {
	conn *amqp.Connection
}

/**
* Создание объекта
*
* @param *amqp.Connection
*/
func NewPublisher(conn *amqp.Connection) *Publisher {
	return &Publisher{
		conn: conn,
	}
}

func (pub *Publisher) Close() {
	go pub.conn.Close()
}

/**
* Публикация данных в очередь
*/
func (pub *Publisher) Publish(queue_name string, message []byte) (bool, error) {

	fmt.Println("Queue to publish:", queue_name)

	c, err := pub.conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %v", err)
		return false, err
	}

	defer c.Close()

	// Declare queue if not exists
	Queue, err := c.QueueDeclare(
		queue_name,
		true,
		false,
		false,
		false,
		amqp.Table{},
	)

	if err != nil {
		log.Fatalf("Declare queue interrupt: %v", err)
		return false, err
	}

	// Publish message to queue
	err = c.Publish(
		"",
		Queue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			Body:         message,
		},
	)

	if err != nil {
		log.Fatalf("basic.publish: %v", err)
		return false, err
	}

	return true, nil
}