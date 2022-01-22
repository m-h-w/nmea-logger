package transform

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Transform data from the B&G logger into a format that Mongo (or another timeseries DB) can
// work with and remove all the extraneous feilds from the data captured by the logger.

// The document metadat contains the only fields that can be changed once the document is written
// more info: https://docs.mongodb.com/manual/core/timeseries/timeseries-limitations/

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

	// map input data to boatSpeed_t var

	// timestamp needs to be converted from and rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	check(err)

	boatSpeed.Ts = t
	boatSpeed.Metadata.DataSource = "log" //data source indictaes which senso the data comes from

	// extract the actual data reading from the sensor. Probably needs to be a function
	fields := input["fields"].(map[string]interface{})
	boatSpeed.IndicatedBoatSpeed = fields["Speed Water Referenced"].(float64)

	// test
	fmt.Println(boatSpeed)

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

	// Try to open the named input file
	ifile, err := os.Open(ipfile)
	check(err)

	// Close file on exit of this function
	defer ifile.Close()

	//  Scan the input file.
	scanner := bufio.NewScanner(ifile)

	for scanner.Scan() {
		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var result map[string]interface{}

		//unmarshall the B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/
		err := json.Unmarshal([]byte(scanner.Text()), &result)

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data format so skip this iteration and move on.
		}

		// all the functions called from this switch statement are concurrent goroutines
		switch result["description"] {

		case "Speed":
			go transformSpeed(result)

		default:
			continue // skip this row as we dont want it stored in the DB

		}

	} //iterate until EOF or error

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input file:", err)
		os.Exit(1)
	}

}
