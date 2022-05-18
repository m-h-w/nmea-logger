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

// Position Data

type PositionMetadata_t struct {
	DataSource string `bson:"source"`
}

type PositionData_t struct {
	Id       string             `bson:"_id,omitempty"`
	Ts       time.Time          `bson:"ts"`
	Metadata PositionMetadata_t `bson:"metadata"`
	Lat      float64            `bson:"lat"`
	Long     float64            `bson:"long"`
}

func transformPositionData(input map[string]interface{}, collection string) {

	var position PositionData_t

	// convert time stamp
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)
	position.Ts = t

	// There maybe a data source from the Sailmon GPS at some later stage
	position.Metadata.DataSource = "B&G GPS"

	//extract the readings
	fields := input["fields"].(map[string]interface{})
	position.Lat = fields["Latitude"].(float64)
	position.Long = fields["Longitude"].(float64)

	//write to data store
	bsonPosition, err := bson.Marshal(position)
	check(err)
	mongodb.WriteToMongo(bsonPosition, collection)

}

// Attitude Data

type attitudeMetadata_t struct {
	DataSource string `json:"source"`
}

type attitudeData_t struct {
	Ts       time.Time          `json:"ts"`
	Metadata attitudeMetadata_t `json:"metadata"`
	Pitch    float64            `json:"pitch"`
	Roll     float64            `json:"roll"`
}

func transformAttitudeData(input map[string]interface{}, collection string) {

	var attitude attitudeData_t

	// convert time stamp
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)
	attitude.Ts = t

	// set the meta data field - there may be an attitude source from the Sailmon later.
	attitude.Metadata.DataSource = "B&G Heel Sensor"

	//extract the readings
	fields := input["fields"].(map[string]interface{})
	attitude.Pitch = fields["Pitch"].(float64)
	attitude.Roll = fields["Roll"].(float64)

	//write to data store
	bsonAttitude, err := bson.Marshal(attitude)
	check(err)
	mongodb.WriteToMongo(bsonAttitude, collection)

}

// Wind Data

type windMetadata_t struct {
	DataSource      string  `json:"source"`
	Reference       string  `json:"reference"`
	AngleCorrection float64 `json:"angleCorrection"`
	SpeedCorrectiom float64 `json:"speedCorrection"`
}

type windData_t struct {
	Ts       time.Time      `json:"ts"`
	Metadata windMetadata_t `json:"metadata"`
	Angle    float64        `json:"windangle"`
	Speed    float64        `json:"windspeed"`
}

func transformWindData(input map[string]interface{}, collection string) {

	var wind windData_t

	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	// fill out the data structure
	wind.Ts = t
	wind.Metadata.DataSource = "Windex"
	wind.Metadata.Reference = "Apparent"

	//extract the readings
	fields := input["fields"].(map[string]interface{})

	// Check reference is Apparent and ignore if not
	reference := fields["Reference"].(map[string]interface{})
	if reference["name"].(string) != "Apparent" {

		if debug {
			fmt.Printf("Wind reading recieved with Apparent not set as reference")
		}
		return
	}

	wind.Angle = fields["Wind Angle"].(float64)
	wind.Speed = fields["Wind Speed"].(float64)

	//write to data store
	bsonWind, err := bson.Marshal(wind)
	check(err)
	mongodb.WriteToMongo(bsonWind, collection)

}

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

func transformCogAndSog(input map[string]interface{}, collection string) {

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

	// check the reference for COG is true (as opposed to magnetic)
	reference := fields["COG Reference"].(map[string]interface{})
	if reference["name"].(string) != "True" {
		if debug {
			fmt.Printf("Cog and Sog reading recieved with True not set as reference")
		}
		return
	}

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
	mongodb.WriteToMongo(bsonSog, collection)

	bsonCog, err := bson.Marshal(cog)
	check(err)
	mongodb.WriteToMongo(bsonCog, collection)

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

func transformHeading(input map[string]interface{}, collection string) {

	var heading heading_t

	// timestamp needs to be converted from an rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	// extract the sensor readings (magnetic heading and magneting variation)
	heading.Ts = t
	heading.Metadata.DataSource = "compass"
	fields := input["fields"].(map[string]interface{})
	heading.MagHeading = fields["Heading"].(float64)

	// variation field doesnt always exis
	if val, ok := fields["Variation"]; ok {
		heading.Metadata.MagVar = val.(float64)
		heading.Metadata.TrueHeading = heading.MagHeading + heading.Metadata.MagVar
	}

	//Marshall the bheading data in bson
	bsonHeading, err := bson.Marshal(heading)
	check(err)
	// write to the data store
	mongodb.WriteToMongo(bsonHeading, collection)
}

// Boat Speed
type boatSpeedMetadata_t struct {
	DataSource         string  `json:"source"`
	CorrectedBoatSpeed float64 `json:"correctedboatspeed"`
}

type boatSpeed_t struct {
	Ts                 time.Time           `json:"ts"`        // timestamp
	Metadata           boatSpeedMetadata_t `json:"metadata"`  // Information about the reading and any corrections
	IndicatedBoatSpeed float64             `json:"boatspeed"` // indicated boatspeed from the log
}

func transformSpeed(input map[string]interface{}, collection string) {

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
	mongodb.WriteToMongo(bsonBoatSpeed, collection)
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

func TransformToMongoFormat(ipfile string, collection string) {

	var i int // debug iteration counter

	// Try to open the named input file
	ifile, err := os.Open(ipfile)
	check(err)

	// Close file on exit of this function
	defer ifile.Close()

	// open the DB Connection
	mongodb.InitMongoConnection()
	// close connection on exit
	defer mongodb.CloseMongoConnection(collection)

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
		if debug {
			fmt.Printf("document: %s\r\n", result["description"])
		}

		switch result["description"] {

		case "Speed":
			transformSpeed(result, collection)

		case "Vessel Heading":
			transformHeading(result, collection)

		case "COG & SOG, Rapid Update":
			transformCogAndSog(result, collection)

		case "Wind Data":
			transformWindData(result, collection)

		case "Position, Rapid Update":
			transformPositionData(result, collection)

		case "Attitude":
			transformAttitudeData(result, collection)

		default:
			continue // skip this row as we dont want it stored in the DB

		}

	} //iterate until EOF or error

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input file:", err)
		os.Exit(1)
	}

}
