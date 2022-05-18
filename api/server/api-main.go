package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	apimongo "github.com/m-h-w/nmea-logger/api/dbdriver"
	api "github.com/m-h-w/nmea-logger/api/endpoints"
)

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the sailing data logger HomePage!")
	fmt.Println("Endpoint Hit: homePage")
}

func boatPosition(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Endpoint Hit: /boat/position")
	api.GetPosition(w, r)
}

func handleRequests() {
	// creates a new instance of a mux router
	Router := mux.NewRouter().StrictSlash(true)

	Router.HandleFunc("/", homePage)
	Router.HandleFunc("/boat/position", boatPosition)

	log.Fatal(http.ListenAndServe(":10000", Router))
}

// The main entry point for the API server
func main() {
	fmt.Println("Rest API v0.1 ")

	apimongo.InitDB()     // connect to the DB
	handleRequests()      // Deal with the requests
	apimongo.ShutDownDB() // shutdown the DB connection

	// ToDo - maybe there is a need to set up a Go Routine to ping the DB
	// periodically and reconnect if it dies... Need to read up some more.

}
