package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
)

// This should probably be in a separate transform module.

// Transform data from the B&G logger into a format that Mongo (or another timeseries DB) can
// work with and remove all the extraneous feilds from the data captured by the logger.

// The document metadat contains the only fields that can be changed once the document is written
// more info: https://docs.mongodb.com/manual/core/timeseries/timeseries-limitations/

type boatSpeedMetadata_t struct {
	DataSource         string  `json:"source"`
	CorrectedBoatSpeed float64 `json:"correctedBoatSpeed"`
}

type boatSpeed_t struct {
	Ts                 string              `json:"ts"`                 // timestamp
	Metadata           boatSpeedMetadata_t `json:"metadata"`           // Information about the reading and any corrections
	IndicatedBoatSpeed float64             `json:"indicatedBoatSpeed"` // indicated boatspeed from the log
}

func transformSpeed(input map[string]interface{}, output *os.File) {

	var boatSpeed boatSpeed_t

	// map input data to boatSpeed_t var
	boatSpeed.Ts = convertToDateFormat(input["timestamp"].(string))
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

func transformToMongoformat(ipfile string) {

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

var debug bool = true

// structure containing any settings entered from the command line
type commandLineSettings_t struct {
	help       bool   // prints out usage info
	transform  bool   // transforms the data from the b&g logger to a more db friendly format
	dispFields bool   // display the different data fields in the logger data
	file       string // take input from a file: -file <filename>
}

func parseCommandLine() *commandLineSettings_t {

	// List the command line options
	helpPtr := flag.Bool("h", false, "prints out usage info")
	transformPtr := flag.Bool("t", false, "transform the input file to a db friendly format")
	dispFieldPtr := flag.Bool("f", false, "Display the different data fields contained in the input file")
	fileNamePtr := flag.String("file", "", "take input from a file -file <filename> ")

	flag.Parse()

	settings := new(commandLineSettings_t)

	settings.help = *helpPtr
	settings.transform = *transformPtr
	settings.file = *fileNamePtr
	settings.dispFields = *dispFieldPtr

	return settings
}

func convertToDateFormat(date string) string {

	// date string is in this format: 	2021-07-09-13:40:59.530"
	// and needs to be in this format:	2021-07-09T13:40:59.530Z"

	date += "Z"
	byteArray := []byte(date)
	byteArray[10] = 0x54 // ascii for 'T' at the 10th (starting from 0) position in the array

	return string(byteArray)
}

// based on https://www.sohamkamani.com/golang/parsing-json/
func modifyTimeStamp(jsonString string) (string, error) {

	//create a map of strings to empty interfaces:
	var result map[string]interface{}
	err := json.Unmarshal([]byte(jsonString), &result)

	if err != nil {
		return "", err
	}
	if result["timestanp"] != nil {
		// If it exists, the object stored in the "timestamp" key is a string
		date := result["timestamp"].(string)

		// Convert "2021-07-09-13:40:59.530" to standard date format
		date = convertToDateFormat(date)
		result["timestamp"] = date

		newJson, err := json.Marshal(result)
		return string(newJson), err
	}
	return jsonString, err //ToDo this is an error state - need to handle better.
}

// looks at the json data from the logger and counts the number of unique descriptions. Descriptions are
// the different sensor types that get logged from the B&G

func countDescriptions(filename string) []string {

	// Try to open the named file
	file, err := os.Open(filename)

	// Error if it wont open
	if err != nil {
		log.Fatal(err)
	}
	// Close file on exit of this function
	defer file.Close()

	// Get next line of file
	scanner := bufio.NewScanner(file)

	//i := 0

	// slice of strings to store unique descriptions in.
	var description []string
	var found bool

	for scanner.Scan() {

		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var result map[string]interface{}

		//unmarshall the B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/
		err := json.Unmarshal([]byte(scanner.Text()), &result)

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data so skip this iteration.
		}

		found = false
		if result["description"] != nil {

			// have a look through previously stored descriptions to see if we have stored this one yet
			for _, v := range description {
				if v == result["description"].(string) {
					found = true // the description has been seen before and stored
				}
			}
			// if not, this is description we havent seen so store it
			if found == false {
				description = append(description, result["description"].(string))
			}

		}

		// Deubug - go round a few times and stop
		/*
			i++
			if i > 20 {
				fmt.Printf("descriptions found %v\r\n", description)
				os.Exit(1)
			}*/

	} // Repeat until EOF or error (scanner keeps scanning until either EOF or error)

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
		os.Exit(1)
	}

	return description
}

func main() {

	settings := parseCommandLine()

	// Display usage information
	if settings.help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Display the data fileds contained in the input file
	if settings.dispFields {

		if settings.file != "" {
			fmt.Printf("counting descriptions\r\n")
			descriptions := countDescriptions(settings.file)

			for i, v := range descriptions {
				fmt.Printf("\r\ndescription[%d] == %v", i, v)
			}
			os.Exit(1)
		} else {

			fmt.Printf(" -f must be used in conjunction with -file <filename>\r\n")
			os.Exit(1)
		}
	}

	// Transform Data to MongoDB format
	if settings.transform {
		if settings.file != "" {

			fmt.Printf("transforming file to MongDB format\r\n")
			transformToMongoformat(settings.file)
			os.Exit(1)

		} else {

			fmt.Printf(" -t must be used in conjunction with -file <filename>\r\n")
			os.Exit(1)

		}
	}
}
