package transform

// Convert the output from the B&G data logger into a format that can be uploaded to the sailnjord data platform.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/atedja/go-vector"
)

func storeTimestamp(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Timestamp: ")
	}

	// this function is defined  in mongotransformer.go but because it is in the same package as this file it is in scope.
	dataStore["ISODateTimeUTC"] = convertToDateFormat(loggerData["timestamp"].(string))

	if debug {
		fmt.Printf("%s\n", dataStore["ISODateTimeUTC"])
	}

}

func storePosition(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Position:")
	}

	fields := loggerData["fields"].(map[string]interface{})
	dataStore["Lat"] = fields["Latitude"].(float64)
	dataStore["Lon"] = fields["Longitude"].(float64)

	if debug {
		fmt.Printf("lat:%f Lon:%f\n", dataStore["Lat"], dataStore["Lon"])
	}

	// use the timestamp from the 2nd position data reading for all the readings between 2 timestamps
	// timestamps seem to appear at ~7ms followed by ~95ms, weirdly. Baically there are 2 readings per ~100ms
	// so there is probably some jitter because of varying processor loads.
	storeTimestamp(loggerData, dataStore)

}

func storeSpeed(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Speed:")
	}

	fields := loggerData["fields"].(map[string]interface{})

	// if there is more than 1 boatspeed between two position readings then the last one will win
	// ToDo: look at averaging
	dataStore["BoatSpeed"] = fields["Speed Water Referenced"].(float64)

	if debug {
		fmt.Printf("%f\n", dataStore["BoatSpeed"])
	}
}

func storeHeading(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Heading: ")
	}

	fields := loggerData["fields"].(map[string]interface{})
	dataStore["Heading"] = fields["Heading"].(float64)

	if debug {
		fmt.Printf("%f\n", dataStore["Heading"])
	}

}

func calculateTrueWindData(dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Calculating True Wind ")
	}

	if _, ok := dataStore["SOG"]; ok { // depending on how the readings arrive we might not have SOG & COG in the first few reqadings

		boatVector := vector.NewWithValues([]float64{dataStore["SOG"].(float64), dataStore["COG"].(float64)})
		windVector := vector.NewWithValues([]float64{dataStore["AWS"].(float64), dataStore["AWA"].(float64)})
		trueWindVector := vector.Subtract(windVector, boatVector)

		if debug {
			fmt.Printf("TWS: %f TWA %f\n", trueWindVector[0], trueWindVector[1])
		}
	} else {
		if debug {
			fmt.Printf("no SOG & COG values yet\n")
		}
	}

}

func storeWindData(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Apparent Wind Angle & Speed ")
	}

	fields := loggerData["fields"].(map[string]interface{})
	dataStore["AWA"] = fields["Wind Angle"].(float64)
	dataStore["AWS"] = fields["Wind Speed"].(float64)

	if debug {
		fmt.Printf("AWA: %f AWS: %f\n", dataStore["AWA"], dataStore["AWS"])
	}

	// calculate true wind angle and true windspeed from apparent wind speed and andgle and COG & SOG
	calculateTrueWindData(dataStore)
}

func storeCOGandSOG(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing COG & SOG ")
	}

	fields := loggerData["fields"].(map[string]interface{})
	dataStore["COG"] = fields["COG"].(float64)
	dataStore["SOG"] = fields["SOG"].(float64)

	if debug {
		fmt.Printf("COG:%f SOG:%f\n", dataStore["COG"], dataStore["SOG"])
	}

}

/* State Machine */
/*---------------*/

type State int

const (
	syncing             State = iota //Looking for the first position reading from the B&G
	storingBGdataPoints              //Looking for subsequent position readings and storing readings of interest.
	/*formattingOutput*/ // this turned out not to work as the transtion event of a new row of data caused the data to be lost during the transition.
)

// look for the first "Position, Rapid Update" json document in the B&G logger output stream
func sync(loggerData map[string]interface{}) State {

	if loggerData["description"] == "Position, Rapid Update" {
		return storingBGdataPoints // change state when the first rapid update position is found
	} else {

		return syncing
	}

}

// Stores the data points we are interested in until the next  position reading comes in, causing
// a state change to the formatting output state. Each data point will have its own timestamp and the
// closest (in time) position reading added to it. If the map is not reset so if there is no data value
// for a particular filed the previous one will be used.

/*************************************************************/
// ToDo refactor so that the state machine ist such a mess!!!!
// this function should be the top of the statemachine.
/************************************************************/

func storingDataPoints(loggerData map[string]interface{}, dataStore map[string]interface{}, datawriter *bufio.Writer) State {

	if debug {
		fmt.Printf("logger data: %s\n", loggerData["description"])
	}

	switch loggerData["description"] {

	case "Position, Rapid Update": // recieving a position reading is the event that generates a state change.

		storePosition(loggerData, dataStore)
		formattingSnOutput(dataStore, datawriter) // write newly accumulated data to output file.

	case "Speed":
		if debug {
			fmt.Printf("%s\n", loggerData)
		}
		storeSpeed(loggerData, dataStore)

	case "Vessel Heading":
		if debug {
			fmt.Printf("%s\n", loggerData)
		}
		storeHeading(loggerData, dataStore)

	case "Wind Data":
		storeWindData(loggerData, dataStore)
		if debug {
			fmt.Printf("%s\n", loggerData)
		}

	case "COG & SOG, Rapid Update":
		storeCOGandSOG(loggerData, dataStore)

	default: // we are not interested in these values so return the current state.
		return storingBGdataPoints

	}
	return storingBGdataPoints // stay in the current storing state.
}

// first row in CSV needs to decalare all the columns. Time and position are mandatory, the others are optional
// The current columns are:
// 		ISODateTimeUTC,Lat,Lon, BoatSpeed, Heading

var columns = [...]string{"ISODateTimeUTC", "Lat", "Lon", "BoatSpeed", "Heading"}

// Put output in CSV format as per https://www.sailnjord.com/data-sources/csv/
func formattingSnOutput(dataStore map[string]interface{}, datawriter *bufio.Writer) State {

	var row string

	// look through the data store and compare with column table to see if they are all present or not. The order is important here as
	// sail njord is expecting the order defined in columns above
	for _, column := range columns {

		/*if debug {
			fmt.Printf("Formatting Data Point %s\n", column)
		}*/

		switch column {
		case "ISODateTimeUTC": // mandatory field
			if _, ok := dataStore["ISODateTimeUTC"]; ok {

				row += dataStore["ISODateTimeUTC"].(string)

			} else {

				fmt.Printf("no timestamp found in data")
				os.Exit(1)
			}
		case "Lat": // mandatory field
			if _, ok := dataStore["Lat"]; ok {

				row += ","
				row += fmt.Sprintf("%f", dataStore["Lat"].(float64))
			} else {
				fmt.Printf("no lattitude found in data")
				os.Exit(1)
			}
		case "Lon": // mandatory field
			if dataStore["Lon"] != nil {

				row += ","
				row += fmt.Sprintf("%f", dataStore["Lon"].(float64))
			} else {
				fmt.Printf("no longitude found in data")
				os.Exit(11)
			}

		case "BoatSpeed": // optional column
			if dataStore["BoatSpeed"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["BoatSpeed"].(float64))
			} else {
				row += ","
			}
		case "Heading": // optional column
			if dataStore["Heading"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["Heading"].(float64))
			} else {
				row += ","
			}

		} //switch
	} //for

	// write to output file
	_, err := datawriter.WriteString(row + "\n")
	check(err)

	return storingBGdataPoints // change state to start storing again.
}

/* Main Entry Point */

// Takes input file from B&G, finds the one second position data and averages the boatspeed & Heading data in between.
// Ouput is a csv in sailnjord format (https://www.sailnjord.com/data-sources/csv/)
// Each row has a tiomestamp and a position measurement.

func SailNjordConverter(file string) {

	var s State = syncing
	var header string // the data schema in the csv.

	dataStore := make(map[string]interface{}) // This is where the readings we care about are stored

	// Try to open the named file
	ipfile, err := os.Open(file)

	// Error if it wont open
	check(err)

	// create the output file with the .sn extention (sailnjord)
	opfile, err := os.OpenFile((file + ".sn"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	// Error if it wont open
	check(err)

	datawriter := bufio.NewWriter(opfile)

	// write the data schema to the file as comma separated headers
	for i := 0; i < len(columns); i++ {
		header += columns[i]

		// dont add a comma after the last header
		if i != (len(columns) - 1) {
			header += ","
		}
	}

	_, err = datawriter.WriteString(header + "\n")
	check(err)

	// Close file on exit of this function
	defer ipfile.Close()
	defer opfile.Close()

	// Get next line of file
	scanner := bufio.NewScanner(ipfile)

	for scanner.Scan() { // read the input file line by line until EOF or error

		//create a map of strings to empty interfaces to unmarshall json B&G logger data into
		var bgJsonInput map[string]interface{}

		//unmarshall  a row of B&G data. Based on https://www.sohamkamani.com/golang/parsing-json/
		err := json.Unmarshal([]byte(scanner.Text()), &bgJsonInput)

		if err != nil {
			fmt.Fprintln(os.Stderr, "error unmarshalling logger data", err)
			continue // error in the input data so skip this iteration.
		}

		if debug {
			fmt.Printf("state = %v\n", s)
		}
		// execute this statemachine to parse through the input datafile and extract what we are interested in.
		switch s {

		case syncing:
			s = sync(bgJsonInput)

		case storingBGdataPoints:
			s = storingDataPoints(bgJsonInput, dataStore, datawriter)

			/*	case formattingOutput:
				s = formattingSnOutput(dataStore, datawriter)
			*/
		}
	}

	datawriter.Flush() // write any lingering data to the file before it is closed.
}
