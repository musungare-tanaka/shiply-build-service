package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	fmt.Println("✓ Successfully connected to RabbitMQ")

	// Create a channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	fmt.Println("✓ Successfully opened channel")

	// Declare the exchange (must match your Spring app)
	err = ch.ExchangeDeclare(
		"shiply_exchange", // name
		"topic",           // type - MUST match your Java TopicExchange
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare exchange")

	fmt.Println("✓ Successfully declared exchange: shiply_exchange")

	// Declare the queue
	q, err := ch.QueueDeclare(
		"shiply",    // name - must match your Spring app
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare queue")

	fmt.Println("✓ Successfully declared queue: shiply")

	// Bind queue to exchange
	err = ch.QueueBind(
		q.Name,                // queue name
		"routing_key_demo",    // routing key - must match your Spring app
		"shiply_exchange",     // exchange name
		false,                 // no-wait
		nil,                   // arguments
	)
	failOnError(err, "Failed to bind queue to exchange")

	fmt.Println("✓ Successfully bound queue to exchange with routing key: routing_key_demo")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// Consume messages
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-acknowledge
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to register a consumer")

	fmt.Println("✓ Consumer started, waiting for messages...")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("\n📨 Received message:\n")
			fmt.Printf("   Body: %s\n", d.Body)
			fmt.Printf("   Content Type: %s\n", d.ContentType)
			fmt.Printf("   Delivery Tag: %d\n", d.DeliveryTag)

			// Acknowledge the message after processing
			err := d.Ack(false)
			if err != nil {
				log.Printf("Failed to acknowledge message: %s", err)
			} else {
				fmt.Println("   ✓ Message acknowledged")
			}
		}
	}()

	<-forever
}