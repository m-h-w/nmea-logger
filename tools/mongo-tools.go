package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/m-h-w/nmea-logger/transform"
)

//This code is a test harness that drives the various modules that transform data and write/read from the DB
//using the command line for input.

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

// looks at the json data from the logger and counts the number of unique descriptions. Descriptions are
// the different sensor types that get logged from the B&G

func countDescriptions(filename string) map[string]int {

	var found bool
	var m = map[string]int{} // map to store the decription (reading type) and the number found

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
			if found == false {
				m[result["description"].(string)] = 1 // this is the first time a description has found
			}

		}

	} // Repeat until EOF or error (scanner keeps scanning until either EOF or error)

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
		os.Exit(1)
	}

	return m
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

			for k, v := range descriptions {
				fmt.Printf("\r\n %s has %v instances", k, v)
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
			transform.TransformToMongoFormat(settings.file) // uses the function in the transform module
			os.Exit(1)

		} else {

			fmt.Printf(" -t must be used in conjunction with -file <filename>\r\n")
			os.Exit(1)

		}
	}
}
