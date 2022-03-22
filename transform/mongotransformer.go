package transform

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/m-h-w/nmea-logger/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

var debug bool = true

// Transform data from the B&G logger into a format that Mongo (or another timeseries DB) can
// work with and remove all the extraneous feilds from the data captured by the logger.

// The document metadata contains the only fields that can be changed once the document is written
// more info: https://docs.mongodb.com/manual/core/timeseries/timeseries-limitations/

//-----------------------------------------------------------------------------------------------//

// GPS Speed and Course

type sogMetadata_t struct {
	DataSource string `json:"source"`
}

type sog_t struct {
	Ts       time.Time     `json:"ts"`
	Metadata sogMetadata_t `json:"metadata"`
	Sog      float64       `json:"sog"`
}

type cogMetadata_t struct {
	DataSource string `json:"source"`
	Ref        string `json:"ref"` //magnetic or true - default seems to be true
}

type cog_t struct {
	Ts       time.Time     `json:"ts"`
	Metadata cogMetadata_t `json:"metadata"`
	Cog      float64       `json:"cog"`
}

func transformCogAndSog(input map[string]interface{}) {

	var cog cog_t
	var sog sog_t

	// timestamp needs to be converted from an rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	// write timestamps
	cog.Ts = t
	sog.Ts = t

	// write constants
	cog.Metadata.DataSource = "gps"
	sog.Metadata.DataSource = "gps"
	cog.Metadata.Ref = "true"

	// extract the gps readings
	fields := input["fields"].(map[string]interface{})

	// test to see if there is a COG value in the incoming string - there isnt always
	if val, ok := fields["COG"]; ok {
		cog.Cog = val.(float64)
	}

	if val, ok := fields["SOG"]; ok {
		sog.Sog = val.(float64)
	}

	// write cog and sog to the data store
	bsonSog, err := bson.Marshal(sog)
	check(err)
	mongodb.WriteToMongo(bsonSog)

	bsonCog, err := bson.Marshal(cog)
	check(err)
	mongodb.WriteToMongo(bsonCog)

}

// Compass Heading
type headingMetadata_t struct {
	DataSource  string  `json:"source"`
	MagVar      float64 `json:"magvar"`
	TrueHeading float64 `json:"trueheading"`
}

type heading_t struct {
	Ts         time.Time         `json:"ts"`
	Metadata   headingMetadata_t `json:"metadata"`
	MagHeading float64           `json:"magheading"`
}

func transformHeading(input map[string]interface{}) {

	var heading heading_t

	// timestamp needs to be converted from an rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	// extract the sensor readings (magnetic heading and magneting variation)
	heading.Ts = t
	heading.Metadata.DataSource = "compass"
	fields := input["fields"].(map[string]interface{})
	heading.MagHeading = fields["Heading"].(float64)

	if val, ok := fields["Variation"]; ok {
		heading.Metadata.MagVar = val.(float64)
		heading.Metadata.TrueHeading = heading.MagHeading + heading.Metadata.MagVar
	}

	//Marshall the bheading data in bson
	bsonHeading, err := bson.Marshal(heading)
	check(err)
	// write to the data store
	mongodb.WriteToMongo(bsonHeading)
}

// Boat Speed
type boatSpeedMetadata_t struct {
	DataSource         string  `json:"source"`
	CorrectedBoatSpeed float64 `json:"correctedBoatSpeed"`
}

type boatSpeed_t struct {
	Ts                 time.Time           `json:"ts"`                 // timestamp
	Metadata           boatSpeedMetadata_t `json:"metadata"`           // Information about the reading and any corrections
	IndicatedBoatSpeed float64             `json:"indicatedBoatSpeed"` // indicated boatspeed from the log
}

func transformSpeed(input map[string]interface{}) {

	var boatSpeed boatSpeed_t

	// timestamp needs to be converted from an rfc3339 string to the golang time.Time type
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
	// write to the data store
	mongodb.WriteToMongo(bsonBoatSpeed)
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

	var i int // debug iteration counter

	// Try to open the named input file
	ifile, err := os.Open(ipfile)
	check(err)

	// Close file on exit of this function
	defer ifile.Close()

	// open the DB Connection
	mongodb.InitMongoConnection()
	// close connection on exit
	defer mongodb.CloseMongoConnection()

	//  Scan the input file.
	scanner := bufio.NewScanner(ifile)

	for scanner.Scan() {
		if debug {
			fmt.Printf("Interation: %d\n", i)
			i++
		}

		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var result map[string]interface{}

		//unmarshall the B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/

		err := json.Unmarshal([]byte(scanner.Text()), &result) //NB result passed by refence

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data format so skip this iteration and move on.
		}

		// all the functions called from this switch statement are concurrent goroutines.
		// NB result is passed by value, not by reference, and so will be thread safe.
		switch result["description"] {

		case "Speed":
			transformSpeed(result)

		case "Vessel Heading":
			transformHeading(result)

		case "COG & SOG, Rapid Update":
			transformCogAndSog(result)

		default:
			continue // skip this row as we dont want it stored in the DB

		}

	} //iterate until EOF or error

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input file:", err)
		os.Exit(1)
	}

}
