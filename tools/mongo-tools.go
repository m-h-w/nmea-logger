package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/m-h-w/nmea-logger/mongodb"
	"github.com/m-h-w/nmea-logger/transform"
)

//This code is a test harness that drives the various modules that transform data and write/read from the DB
//using the command line for input.

// structure containing any settings entered from the command line
type commandLineSettings_t struct {
	help        bool   // prints out usage info
	transform   bool   // transforms the data from the b&g logger to a more db friendly format
	dispFields  bool   // display the different data fields in the logger data
	file        string // take input from a file: -file <filename>
	sailNjord   bool   // convert B&G output file to Sailnjord format for core readings
	collection  string // specify the collection to write to
	lowResTable bool   // generate a low resolution table to help the UI scale.
}

func parseCommandLine() *commandLineSettings_t {

	// List the command line options
	helpPtr := flag.Bool("h", false, "Prints out usage info")
	transformPtr := flag.Bool("t", false, "Transform the input file to a db friendly format")
	dispFieldPtr := flag.Bool("f", false, "Display the different data fields contained in the input file")
	fileNamePtr := flag.String("file", "", "Take input from a file -file <filename> ")
	sailNjordPtr := flag.Bool("sn", false, "Transform fileinput to Sail Njord format")
	colPtr := flag.String("col", "", "Collection (Table) to write to in the DB")
	lowResPtr := flag.Bool("l", false, "generates a low resolution table, with default resolution 6 seconds")

	flag.Parse()

	settings := new(commandLineSettings_t)

	settings.help = *helpPtr
	settings.transform = *transformPtr
	settings.file = *fileNamePtr
	settings.dispFields = *dispFieldPtr
	settings.sailNjord = *sailNjordPtr
	settings.collection = *colPtr
	settings.lowResTable = *lowResPtr

	return settings
}

// looks at the json data from the logger and counts the number of unique descriptions. Descriptions are
// the different sensor types that get logged from the B&G

func countDescriptions(filename string) map[string]int {

	var found bool
	var m = map[string]int{} // map to store the decription (reading type) and the number found
	m["interations"] = 0

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
			for k, _ := range m {
				if k == result["description"].(string) {
					found = true // the description has been seen before and stored
					m[k]++       // update the count of this description
				}
			}
			// if not, this is description we havent seen so store it
			if !found {
				m[result["description"].(string)] = 1 // this is the first time a description has found
			}

		}

		m["iterations"]++

	} // Repeat until EOF or error (scanner keeps scanning until either EOF or error)

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
		os.Exit(1)
	}

	return m
}

/*

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// The Pi doesnt have a realtime clock and so if it is powered down and back up again without access to wifi
// it loses its time sync. When data is collected it will have the wrong time stamp on it and so we need to correct
// it. Luckily the incomping data stream has a system time value which is sent every second in the "Sytem Time" decription
// This functions looks for the first System time and compares it with the attached time stamp on the document, calculates
// the error and applies it to all the documents in the file.

func timeAlign(ipfile string) {

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
		err := json.Unmarshal([]byte(scanner.Text()), &result) //NB result passed by refence

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data format so skip this iteration and move on.
		}

	}// itereate until EOF or error
}
*/
func main() {

	settings := parseCommandLine()

	// Display usage information
	if settings.help {
		flag.PrintDefaults()
		os.Exit(1)
	} else if settings.dispFields { // Display the data fileds contained in the input file

		if settings.file != "" {
			fmt.Printf("counting descriptions\r\n")
			descriptions := countDescriptions(settings.file)

			for k, v := range descriptions {
				fmt.Printf("\r\n %s has %v instances", k, v)
			}
			os.Exit(1)
		} else {

			fmt.Printf(" -f must be used in conjunction with -file <filename>\r\n")
			os.Exit(1)
		}
	} else if settings.transform { // Transform Data to MongoDB format

		if settings.file != "" && settings.collection != "" {

			//Check if collection already exists and stop if it does. for now it would need to be
			//deleted manually using the Atlas UI.

			colls := mongodb.ListCollections()

			for _, col := range colls {
				if col == settings.collection {
					fmt.Printf("Collection %s exists already \r\n", settings.collection)
					os.Exit(1)
				}
			}

			fmt.Printf("transforming file to MongDB format\r\nWriting to Collection:%s\r\n", settings.collection)
			transform.TransformToMongoFormat(settings.file, settings.collection) // uses the function in the transform module
			os.Exit(1)

		} else {

			fmt.Printf(" -t must be used in conjunction with -file <filename> and -col for a collection\r\n")
			os.Exit(1)

		}
	} else if settings.lowResTable {

		/* build a low resolution data table default = 6 second position data. The thinking here is to drive the UI from a
		map view so that points of interest can be identified spatially and the times at which the happend then used to
		pull back higher resolution data from the main collection.
		*/

		if settings.collection != "" {
			transform.GenerateLowResView(6, settings.collection)
		} else {
			fmt.Printf("-l must be used in conjuction with -col <collection name>")
		}

	} else if settings.sailNjord { // Create a Sail Njord compatible file

		if settings.file != "" {

			fmt.Printf("transforming file to Sail Njord format\r\n")
			transform.SailNjordConverter(settings.file) // uses the function in the transform module
			os.Exit(1)

		} else {

			fmt.Printf(" -sn must be used in conjunction with -file <filename>\r\n")
			os.Exit(1)

		}

	} else { // print the usage if no command line flags provided
		flag.PrintDefaults()
		os.Exit(1)
	}
}
