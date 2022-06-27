package transform

// Convert the output from the B&G data logger into a format that can be uploaded to the sailnjord data platform.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

const ms2knots = 1.944 //convert m/s to knots

func calculateTWS(dataStore map[string]interface{}) float64 {

	/*
	   Using the cosine rule (a^2=b^2+c^2-2bcCosA)
	   TWS = SQRT (BoatSpeed^2 + AWS^2 - 2xBoatSpeedxAWS x Cos AWA)
	*/

	b, ok := dataStore["BoatSpeed"].(float64) // boatspeed may not have been written in the early start up phase

	if !ok {

		return 0
	}

	b2 := b * b

	c2 := dataStore["AWS"].(float64) * dataStore["AWS"].(float64)

	bc := dataStore["BoatSpeed"].(float64) * dataStore["AWS"].(float64)

	awaRads := (dataStore["AWA"].(float64) / 360) * 2 * math.Pi

	tws := math.Sqrt(b2 + c2 - 2*bc*math.Cos(awaRads))

	return tws
}

func calculateTWA(dataStore map[string]interface{}, tws float64) float64 {

	/*
		Use the cosine rule again ...

		Sine rule is harder to use because the angles we need is often >90
		and the sin of that angle has an equivalent angle less than 90 degrees.

		By using the cosine rule on the angle between the apparent and true wind (A)
		which is always less than 90 degrees its more predicatable.
	*/

	// a^2=b^2+c^2 -2bcCosA
	// where a=boatspeed b= apparent wind speed, c= true wind speed and A is the angle between TWS and AWS
	// rearranging gives:
	// cosA = (b^2+c^2-a^2)/2bc
	// We can then find the angle we need by subtracting angle A plus AWA from 180 (sum of internal angles of a triangle)
	// Since the  angle we have just found is the supplementary angle to TWA we need to subtract that from 180
	// TWA = 180 - (180-A-AWA) = A+AWA

	a, ok := dataStore["BoatSpeed"].(float64) // boatspeed may not have been written in the early start up phase

	if !ok {
		return 0
	}

	a2 := a * a
	b2 := dataStore["AWS"].(float64) * dataStore["AWS"].(float64)
	c2 := tws * tws
	bc := tws * dataStore["AWS"].(float64)
	/*
		fmt.Printf("a2:%f\nb2:%f\nc2:%f\nbc:%f\n", a2, b2, c2, bc)
		fmt.Printf("CosA %f  \n", (b2+c2-a2)/2*bc)
	*/
	Arads := math.Acos((b2 + c2 - a2) / (2 * bc))
	Adeg := (Arads / (2 * math.Pi)) * 360

	twa := Adeg + dataStore["AWA"].(float64)

	return twa
}

func storeTrueWindData(dataStore map[string]interface{}, tws float64, twa float64) {

	dataStore["TWS"] = tws // boatspeed and windspeed (inputs to TWS) are in knots so no need to convert from m/s
	dataStore["TWA"] = twa
}

func calculateTrueWindData(dataStore map[string]interface{}) {

	tws := calculateTWS(dataStore) // needs to be done first because tws is needed for twa.

	twa := calculateTWA(dataStore, tws)

	storeTrueWindData(dataStore, tws, twa)
}

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

	fields, ok := loggerData["fields"].(map[string]interface{})

	if !ok { // test type conversion. was getting errors in some of the input data.
		fmt.Printf("got data of type %T but wanted map[string]interface{} ", fields)
		return
	}

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
	dataStore["BoatSpeed"] = fields["Speed Water Referenced"].(float64) * ms2knots

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

func storeWindData(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Apparent Wind Angle & Speed ")
	}

	fields := loggerData["fields"].(map[string]interface{})
	dataStore["AWA"] = fields["Wind Angle"].(float64)
	dataStore["AWS"] = fields["Wind Speed"].(float64) * ms2knots

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

	if _, ok := fields["COG"].(float64); ok { // some times there is no data in the incoming json for some reason.
		dataStore["COG"] = fields["COG"].(float64)
		dataStore["SOG"] = fields["SOG"].(float64) * ms2knots

		if debug {
			fmt.Printf("COG:%f SOG:%f\n", dataStore["COG"], dataStore["SOG"])
		}
	} else {
		if debug {
			fmt.Printf("no COG in incoming jason.\n %v", loggerData)
		}
	}
}

// heel(roll) and pitch
func storeAttitude(loggerData map[string]interface{}, dataStore map[string]interface{}) {

	if debug {
		fmt.Printf("Storing Attitude ")
	}

	fields := loggerData["fields"].(map[string]interface{})

	if _, ok := fields["Pitch"].(float64); ok { // some times there is no data in the incoming json for some reason.
		dataStore["Pitch"] = fields["Pitch"].(float64)
		dataStore["Heel"] = fields["Roll"].(float64)

		if debug {
			fmt.Printf("Pitch:%f Heel:%f\n", dataStore["Pitch"], dataStore["Heel"])
		}
	} else {
		if debug {
			fmt.Printf("no Pitch in incoming json.\n %v", loggerData)
		}
	}
}

/* State Machine */
/*---------------*/

type State int

var prevReadingTimeStamp string

const (
	syncing             State = iota //Looking for the first position reading from the B&G
	storingBGdataPoints              //Looking for subsequent position readings and storing readings of interest.
	/*formattingOutput*/ // this turned out not to work as the transtion event of a new row of data caused the data to be lost during the transition.
)

// look for the first "Position, Rapid Update" json document in the B&G logger output stream
func sync(loggerData map[string]interface{}, dataStore map[string]interface{}) State {

	if loggerData["description"] == "Position, Rapid Update" {

		// store the timestamp from the first position reading as position readings drive the state behaviour
		storePosition(loggerData, dataStore)
		prevReadingTimeStamp = dataStore["ISODateTimeUTC"].(string) // used to control how much data we want per second

		return storingBGdataPoints // change state when the first rapid update position is found
	} else {

		return syncing
	}

}

// Stores the data points we are interested in until the next  position reading comes in, causing
// a state change to the formatting output state. Each data point will have its own timestamp and the
// closest (in time) position reading added to it. The map is not reset so if there is no data value
// for a particular fieled the previous one will be used.

/*************************************************************/
// ToDo refactor so that the state machine ist such a mess!!!!
// this function should be the top of the statemachine.
/************************************************************/

var dataFreq int64 = 1000000000 // The required difference between timestamp readings in nanoseconds

func compareTimeStamps(refTime string, timeNow string) int64 {

	ref, _ := time.Parse(time.RFC3339, refTime)
	now, _ := time.Parse(time.RFC3339, timeNow)
	diff := now.Sub(ref) // returns the number of nano seconds between the two times.

	return int64(diff)
}

func storingDataPoints(loggerData map[string]interface{}, dataStore map[string]interface{}, datawriter *bufio.Writer) State {

	if debug {
		fmt.Printf("logger data: %s\n", loggerData["description"])
	}

	switch loggerData["description"] {

	case "Position, Rapid Update": // recieving a position reading is the event that generates a state change.

		storePosition(loggerData, dataStore)
		if compareTimeStamps(prevReadingTimeStamp, dataStore["ISODateTimeUTC"].(string)) >= dataFreq {

			formattingSnOutput(dataStore, datawriter)                   // write newly accumulated data to output file
			prevReadingTimeStamp = dataStore["ISODateTimeUTC"].(string) //set the new comparison time stamp
		}

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
		storeWindData(loggerData, dataStore) // Triggers calculation of true wind data
		if debug {
			fmt.Printf("%s\n", loggerData)
		}

	case "COG & SOG, Rapid Update":
		storeCOGandSOG(loggerData, dataStore)

	case "Attitude":
		storeAttitude(loggerData, dataStore)

	default: // we are not interested in these values so return the current state.
		return storingBGdataPoints

	}
	return storingBGdataPoints // stay in the current storing state.
}

// first row in CSV needs to decalare all the columns. Time and position are mandatory, the others are optional
// The current columns are:
// 		ISODateTimeUTC,Lat,Lon, BoatSpeed, Heading

var columns = [...]string{"ISODateTimeUTC", "Lat", "Lon", "BoatSpeed", "Heading", "AWA", "AWS", "TWS", "TWA", "COG", "SOG", "Heel", "Pitch"}

// Put output in CSV format as per https://www.sailnjord.com/data-sources/csv/
func formattingSnOutput(dataStore map[string]interface{}, datawriter *bufio.Writer) State {

	var row string

	// look through the data store and compare with column table to see if they are all present or not. The order is important here as
	// sail njord is expecting the order defined in columns above
	for _, column := range columns {

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
		case "AWA":
			if dataStore["AWA"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["AWA"].(float64))
			} else {
				row += ","
			}
		case "AWS":
			if dataStore["AWS"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["AWS"].(float64))
			} else {
				row += ","
			}
		case "TWS":
			if dataStore["TWS"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["TWS"].(float64))
			} else {
				row += ","
			}
		case "TWA":
			if dataStore["TWA"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["TWA"].(float64))
			} else {
				row += ","
			}
		case "COG":
			if dataStore["COG"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["COG"].(float64))
			} else {
				row += ","
			}
		case "SOG":
			if dataStore["SOG"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["SOG"].(float64))
			} else {
				row += ","
			}
		case "Heel":
			if dataStore["Heel"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["Heel"].(float64))
			} else {
				row += ","
			}
		case "Pitch":
			if dataStore["Pitch"] != nil {
				row += ","
				row += fmt.Sprintf("%f", dataStore["Pitch"].(float64))
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

	// create the csv output file
	opfile, err := os.OpenFile((file + "sn.csv"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

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
		// execute this state machine to parse through the input datafile and extract what we are interested in.
		switch s {

		case syncing:
			// Wait for the GPS to start sending position updates as every row needs a
			// a time stamp and a position associated with it
			s = sync(bgJsonInput, dataStore)

		case storingBGdataPoints:
			s = storingDataPoints(bgJsonInput, dataStore, datawriter)
		}
	}

	datawriter.Flush() // write any lingering data to the file before it is closed.
}
