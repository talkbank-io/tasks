package publisher

import (
	//"fmt"
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

/**
* Публикация данных в очередь
*/
func (pub *Publisher) Publish(message []byte) (bool, error) {

	c, err := pub.conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %v", err)
		return false, err
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Headers:         amqp.Table{},
                ContentType:     "application/json",
		Body:         message,
	}

	err = c.Publish("", "core.massaction", false, false, msg)
	if err != nil {
		log.Fatalf("basic.publish: %v", err)
		return false, err
	}

	return true, nil
}