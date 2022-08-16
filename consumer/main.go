package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"movie-history-app/model"
	"net/http"
)

func main() {
	RabbitMqClient, err := NewRabbitMqClient("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer RabbitMqClient.Close()

	for {
		event, err := RabbitMqClient.ConsumeEvents()
		if err != nil {
			fmt.Println("Error when consuming message", err.Error())
			continue
		}

		if err := PostMovie(event); err != nil {
			fmt.Println("Error when posting err", err.Error())
			continue
		}

		fmt.Println(event)
	}
}

func PostMovie(movie model.Movie) error {
	url := "http://localhost:8000" + "/api/v1/stores"
	jsonBytes, _ := json.Marshal(movie)
	request, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBytes))

	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.New("The process could not be completed successfully on the storer side")
	}

	return nil
}
