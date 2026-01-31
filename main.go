package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	Data string  `json: "data"`
}


func getGreeting(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, Server{Data: "Hello, World!"})
}

func main() {
	router := gin.Default()

	router.GET("/greet", getGreeting)

	router.Run("localhost:8081")
}