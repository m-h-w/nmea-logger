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

func transformSpeed(input map[string]interface{}, output *os.File) {

	var boatSpeed boatSpeed_t

	// map input data to boatSpeed_t var

	// timestamp needs to be converted from and rfc3339 string to the golang time.Time type
	t, err := time.Parse(time.RFC3339, convertToDateFormat(input["timestamp"].(string)))
	if err != nil {
		panic(err)
	}
	boatSpeed.Ts = t
	boatSpeed.Metadata.DataSource = "log"

	fields := input["fields"].(map[string]interface{})
	boatSpeed.IndicatedBoatSpeed = fields["Speed Water Referenced"].(float64)

	// build json object and write to file
	data, _ := json.Marshal(boatSpeed)
	_, e := output.Write(data) // write json to file

	check(e)

	if debug == true {
		fmt.Printf("json: %s\r\n", data)
	}

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TransformToMongoFormat(ipfile string) {

	// Try to open the named input file
	ifile, err := os.Open(ipfile)
	check(err)

	// Check if output file exisits and delete it if it does.
	if _, err := os.Stat(ipfile + ".mong"); err == nil {

		if err != nil {
			fmt.Fprintln(os.Stderr, "error check if file exists", err)
			os.Exit(1)
		}

		err := os.Remove(ipfile + ".mong")
		check(err)
	}

	// Try to open the output file and append the .mong extension to it
	ofile, err := os.Create(ipfile + ".mong")
	check(err)

	// Close files on exit of this function
	defer ifile.Close()
	defer ofile.Close()

	// Get next line of file
	scanner := bufio.NewScanner(ifile)

	for scanner.Scan() {
		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var result map[string]interface{}

		//unmarshall the B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/
		err := json.Unmarshal([]byte(scanner.Text()), &result)

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data so skip this iteration.
		}

		switch result["description"] {

		case "Speed":
			transformSpeed(result, ofile)

		default:
			continue // skip this row as we dont want it stored in the DB

		}

	} //iterate until EOF or error

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input file:", err)
		os.Exit(1)
	}

}

// end of possible module.

//----------------------------------------------------------------------------------------------
