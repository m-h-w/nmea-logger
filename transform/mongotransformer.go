package transform

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/m-h-w/nmea-logger/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var debug bool = true

// Transform data from the B&G logger into a format that Mongo (or another timeseries DB) can
// work with and remove all the extraneous feilds from the data captured by the logger.

// The document metadat contains the only fields that can be changed once the document is written
// more info: https://docs.mongodb.com/manual/core/timeseries/timeseries-limitations/

//Data Structures

type boatSpeedMetadata_t struct {
	DataSource         string  `json:"source"`
	CorrectedBoatSpeed float64 `json:"correctedBoatSpeed"`
}

type boatSpeed_t struct {
	Ts                 time.Time           `json:"ts"`                 // timestamp
	Metadata           boatSpeedMetadata_t `json:"metadata"`           // Information about the reading and any corrections
	IndicatedBoatSpeed float64             `json:"indicatedBoatSpeed"` // indicated boatspeed from the log
}

// takes a []byte of bson values for all the document types and caches DB_WRITE_THRESHOLD documents before writing them to
// Mongo using insertMany

func writeToCache(writeCount *mongodb.DbWriteCache_t, v []byte, client *mongo.Client) {

	if debug {
		fmt.Printf("Writing to cache %d \n", writeCount.Count)
	}
	if writeCount.Count == 0 {
		// This is the first time through since the last write to the DB so allocate memory to cache
		// documents in until the number reaches the DB_WRITE_THRESHOLD
		// This cache object will be consumed in a go routine so a new object is required per call to
		// the go routine. Assumption is that garbage collector will free the memory once the go routine
		// terminates.

		writeCount.Mem = new([mongodb.DB_WRITE_THRESHOLD][]byte)
		writeCount.Count = 0 //reset the write  counter for the next 100 documents

		// write the first bson doc to cache
		writeCount.Mem[writeCount.Count] = v
		writeCount.Count++

	} else if writeCount.Count == (mongodb.DB_WRITE_THRESHOLD - 1) { // 0-99 not 1-100

		if debug {
			// Write cache to DB
			fmt.Printf("write cache to DB \n")
		}

		// write the last json doc to cache; but dont update the count
		writeCount.Mem[writeCount.Count] = v
		mongodb.WriteCacheToDB(client, writeCount)

	} else {

		// write bson data to cache
		writeCount.Mem[writeCount.Count] = v
		// update the write count for this document. Write to DB when it reaches DB_WRITE_THRESHOLD (100)
		writeCount.Count++
	}

}

func transformSpeed(input map[string]interface{}, writeCount *mongodb.DbWriteCache_t, client *mongo.Client) {

	var boatSpeed boatSpeed_t

	// timestamp needs to be converted from and rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	// extract the actual data reading from the sensor.
	boatSpeed.Ts = t
	boatSpeed.Metadata.DataSource = "log" //data source indictaes which senso the data comes from
	fields := input["fields"].(map[string]interface{})
	boatSpeed.IndicatedBoatSpeed = fields["Speed Water Referenced"].(float64)

	//Marshall the boatSpeed data in json
	bsonBoatSpeed, err := bson.Marshal(boatSpeed)
	check(err)

	writeToCache(writeCount, bsonBoatSpeed, client)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func convertToDateFormat(date string) string {

	// date string is in this format: 	2021-07-09-13:40:59.530"
	// and needs to be in this format:	2021-07-09T13:40:59.530Z"

	date += "Z"
	byteArray := []byte(date)
	byteArray[10] = 0x54 // ascii for 'T' at the 10th (starting from 0) position in the array
	return string(byteArray)
}

func TransformToMongoFormat(ipfile string) {
	var writeCount mongodb.DbWriteCache_t // this structure manages the cache

	// Try to open the named input file
	ifile, err := os.Open(ipfile)
	check(err)

	// Close file on exit of this function
	defer ifile.Close()

	// open the DB Connection
	mongoClient := mongodb.InitMongoConnection()
	// close connection on exit
	defer mongodb.CloseMongoConnection(mongoClient)

	//  Scan the input file.
	scanner := bufio.NewScanner(ifile)

	for scanner.Scan() {
		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var result map[string]interface{}

		//unmarshall the B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/

		err := json.Unmarshal([]byte(scanner.Text()), &result) //NB result passed by reference

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data format so skip this iteration and move on.
		}

		// all the functions called from this switch statement are concurrent goroutines.
		// NB result is passed by value, not by reference, and so will be thread safe.

		switch result["description"] {

		case "Speed":
			go transformSpeed(result, &writeCount, mongoClient)

		default:
			continue // skip this row as we dont want it stored in the DB

		}

	} //iterate until EOF or error

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input file:", err)
		os.Exit(1)
	}

}
