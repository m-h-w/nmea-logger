package transform

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/m-h-w/nmea-logger/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

/*
This module generates lower resolution views of the data to support the UI scaling in and out
*/

func BuildLowResTable(searchField string, resolution int64, readCol string, writeCol string) {

	var resetTime bool = true
	var result PositionData_t // need to swith on searchField and set the result to the appropriate type
	var timeToWrite time.Time // initialises to 1st Jan 1971 (zero value)
	var i int                 // debug variable

	if debug {
		fmt.Printf("BuildLowResTable. Reset time = %v\n", resetTime)
	}

	cursor := mongodb.ReadAll(searchField, readCol)

	for cursor.Next(context.TODO()) {

		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}

		if result.Ts.After(timeToWrite) { // its > res seconds after the last BD write
			resetTime = true
			fmt.Printf("test\n")
		}

		if resetTime {
			// add the resolution to the current timesatamp
			// to genetate the timestamp for the next write

			timeToWrite = result.Ts.Add(time.Second * time.Duration(resolution))
			resetTime = false

			if debug {
				fmt.Printf("%d. Timestamp: %v lat: %f Lon: %f\n", i, result.Ts, result.Lat, result.Long)
				i++
			}

			// write  to the data store ToDo: Figure out how to manage the connections to two tables
			bsonResult, err := bson.Marshal(result)
			check(err)
			mongodb.WriteToMongo(bsonResult, writeCol)

		}

	}

	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}

func generatePositionView(res int64, readCol string, writeCol string) {

	BuildLowResTable("lat", res, readCol, writeCol) // grab the position data from the main collection

}

// creates low res tables for 1 second, 6 second and 60 second data
func GenerateLowResView(res int64, readCol string) {

	var writeCol string // the collection to write the low res table to
	// as opposed to the readCol, the table we are reading from

	// the suffixes addded here are tested in getPosition.go
	// in the api server package.

	switch res {
	case 1:
		writeCol = readCol + "-one-second"
	case 6:
		writeCol = readCol + "-six-second"
	case 60:
		writeCol = readCol + "-sixty-second"
	default:
		fmt.Printf("Resolution of %d Seconds not supported", res)
		os.Exit(1)
	}

	// check to see if collection exists and stop if it has.
	// Need to drop the collection from Atlas UI before writing again if
	// that is the intention.
	colls := mongodb.ListCollections()

	for _, col := range colls {
		if col == writeCol {
			fmt.Printf("Collection %s exists already \r\n", writeCol)
			os.Exit(1)
		}
	}

	mongodb.InitMongoConnection()
	generatePositionView(res, readCol, writeCol)
	mongodb.CloseMongoConnection(writeCol)

}
