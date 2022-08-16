package main

import (
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"movie-history-app/model"
)

type RabbitMqClient struct {
	Connection        *amqp.Connection
	Channel           *amqp.Channel
	MovieHistoryQueue amqp.Queue
	ConnString        string
	MessageChannel    <-chan amqp.Delivery
	RetryAttempt      int
}

func NewRabbitMqClient(conn string) (*RabbitMqClient, error) {
	Client := &RabbitMqClient{ConnString: conn}

	if err := Client.Connect(Client.ConnString); err != nil {
		return nil, err
	}

	if err := Client.ConfigureQueue(); err != nil {
		return nil, err
	}

	return Client, nil
}

func (rmq *RabbitMqClient) Connect(connString string) error {
	var err error

	rmq.Connection, err = amqp.Dial(connString)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	rmq.Channel, err = rmq.Connection.Channel()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func (rmq *RabbitMqClient) Close() {
	rmq.Channel.Close()
	rmq.Connection.Close()
}

func (rmq *RabbitMqClient) ConfigureQueue() error {
	var err error
	rmq.MovieHistoryQueue, err = rmq.Channel.QueueDeclare(
		"movie_queue",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	err = rmq.Channel.ExchangeDeclare(
		"movie_exchange",
		"fanout",
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	err = rmq.Channel.QueueBind(
		rmq.MovieHistoryQueue.Name,
		"",
		"movie_exchange",
		false,
		nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	rmq.MessageChannel, err = rmq.Channel.Consume(
		rmq.MovieHistoryQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func (rmq *RabbitMqClient) ConsumeEvents() (model.Movie, error) {
	for {
		msg := <-rmq.MessageChannel
		return ProcessEvent(msg.Body)
	}
}

func ProcessEvent(msg []byte) (model.Movie, error) {
	var movie model.Movie
	if err := json.Unmarshal(msg, &movie); err != nil {
		fmt.Println("Cannot unmarshall", err.Error())
		return model.Movie{}, err
	}
	return movie, nil
}
