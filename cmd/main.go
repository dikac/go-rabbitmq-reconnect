package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Config struct {
	username string
	password string
	host     string
	port     int
}

type Channel struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

type Container struct {
	connection *amqp.Connection
}

// dial
// create connection and register channel
func dial(config Config, channels []Channel) (conn *amqp.Connection, err error) {

	// create connection
	conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", config.username, config.password, config.host, config.port))
	if err != nil {
		return
	}
	log.Print("rabbitmq connect success")

	// register channel
	channel, err := conn.Channel()
	for _, c := range channels {
		err = channel.ExchangeDeclare(c.name, c.kind, c.durable, c.autoDelete, c.internal, c.noWait, c.args)
		if err != nil {
			return
		}
	}
	log.Print("rabbitmq channels register success")

	return
}

func check(container *Container) {
	for {
		time.Sleep(time.Duration(1) * time.Second)
		log.Printf("rabbitmq connected: %v", !container.connection.IsClosed())
	}
}

func main() {

	config := Config{
		username: "guest",
		password: "guest",
		host:     "localhost",
		port:     5672,
	}

	channels := []Channel{
		{
			name:       "reconnection-exchange",
			kind:       "direct",
			durable:    true,
			autoDelete: false,
			internal:   false,
			noWait:     false,
			args:       nil,
		},
	}

	container := &Container{}

	connection, err := dial(config, channels)
	if err != nil {
		log.Fatal(err)
		return
	}
	container.connection = connection
	go check(container)

	defer func() {
		if err := container.connection.Close(); err != nil {
			log.Print(err)
		}
		log.Print("rabbitmq connection closed")

	}()

	go func() {
		for {
			reason, ok := <-connection.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Print("rabbitmq connection closed")
				break
			}
			log.Printf("rabbitmq connection closed unexpectedly, reason: %v", reason)

			for {

				time.Sleep(time.Duration(1) * time.Second)

				connection, err = dial(config, channels)

				if err == nil {
					container.connection = connection
					log.Print("rabbitmq reconnect success")
					break
				}

				log.Printf("rabbitmq reconnect failed, err: %v", err)
			}

		}
	}()

	{
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		<-quit

	}
}
