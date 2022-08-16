package main

import (
	"encoding/json"
	"github.com/labstack/echo/v4"
	"movie-history-app/model"
	"net/http"
	"os"
)

func main() {
	e := echo.New()
	e.POST("/api/v1/stores", storeMovie)
	e.Logger.Fatal(e.Start(":8000"))
}

func storeMovie(c echo.Context) error {
	var movie model.Movie
	if err := json.NewDecoder(c.Request().Body).Decode(&movie); err != nil {
		return echo.ErrBadRequest
	}

	filePtr, err := os.OpenFile("./data.csv", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return echo.ErrInternalServerError
	}
	defer filePtr.Close()

	_, err = filePtr.Write([]byte(movie.Title + "," + movie.LastView + "\n"))
	if err != nil {
		return echo.ErrInternalServerError
	}

	return c.NoContent(http.StatusOK)
}
