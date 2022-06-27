package api

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

var devTest bool = true

/*
Query structure
---------------
?start=yymmddT00.00.00.000&stop=yymmddT00.00.00.000 - Find all the tacks (as opposed to gybes) between start and stop
returns a json array of strings with tack times in them.
?table=<tablename>

*/

func GetTackTimes(w http.ResponseWriter, r *http.Request) { // r is the request, w is the response
	var tacks []string
	// parse the request and extract the parameters.
	//https://pkg.go.dev/net/url#URL.Query

	q := r.URL.Query()

	//extract the paramenterss from the url Query string
	start := q.Get("start")
	stop := q.Get("stop")
	table := q.Get("table")

	// check query has been formatted correctly.
	if start == "" || stop == "" {
		w.WriteHeader(http.StatusBadRequest)
	}

	//check stop time is after start time and times are formatted correctly
	startTime, err := time.Parse(time.RFC3339, start)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	stopTime, err := time.Parse(time.RFC3339, stop)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	diff := stopTime.Sub(startTime)
	if diff < 0 {
		w.WriteHeader(http.StatusBadRequest)
	}

	//ToDo check that start and stop times are in the range of the data set in the table

	if devTest { // opens one of the raw files as opposed to querying the DB for testing

		// Try to open the named file
		ipfile, err := os.Open(table)
		// Error if it wont open
		if err != nil {
			log.Printf("Crashed trying to open test file:%s\n", table)
			panic(err)
		}

		scanner := bufio.NewScanner(ipfile)

		for scanner.Scan() {
			var bgJsonInput map[string]interface{}
			err := json.Unmarshal([]byte(scanner.Text()), &bgJsonInput)
			if err != nil {
				log.Printf("error unmarshalling logger data %v", err)
				continue // error in the input data so skip this iteration.
			}
			if bgJsonInput["description"] == "Wind Data" {
				fields := bgJsonInput["fields"].(map[string]interface{})
				ts := bgJsonInput["timestamp"].(string)
				tack := detectTack(fields["Wind Angle"].(float64), ts)

				if tack != "" {
					tacks = append(tacks, tack)
				} // record the timestamp of the tack
			} else {

				continue // data row doesnt contain wind data so ignore
			}
		}

	} else { // this is the actual code that would run on the api server
		log.Printf("write the DB Query to extract data to detect a tack between two time stamps")
		// In some kind of loop
		// data:=queryDB(awa,ts)
		// tacks=detectTack(data)
	}

	log.Printf("tacks at %v", tacks)

}

/*

Pass in a row from the data table and see if there is a tack there.
Note: This function has state behaviour!!! It is assumed that it would execute to completion
during a single API call.

*/

const awastb float64 = 45.00 // if AWA is <180 we are on starboard. Likely it will be <30degs
const awapt float64 = 315.00 // if AWA is > 180 we are on port. Likely it will be >315degs
const depth int = 10         // depth of 10 represents 2 seconds of wind data.Must be even number. When it is 5 pt and 5 stb we can declare a tack
var i int = 0                // index into history array
type history_t struct {
	tack string
	ts   string
}

var history [depth]history_t // the last "depth" tack decisions

func detectTack(awa float64, ts string) string {
	// store the latest decision as to what tack we are on.
	if awa <= awastb {
		history[i].tack = "stb"
		history[i].ts = ts
	} else if awa >= awapt {
		history[i].tack = "pt"
		history[i].ts = ts
	} else {
		history[i].tack = "offwind"
	}
	// manage the index roll over
	if i >= depth {
		i = 0
	} else {
		i++
	}

	// test to see if a tack has occured
	ptcount := 0
	stbcount := 0
	for i := 0; i < depth; i++ {
		if history[i] == "pt" {
			ptcount++
		} else if history[i] == "stb" {
			stbcount++
		}
	}
	if ptcount == stbcount && ptcount == depth/2 {
		return history[depth/2].ts
	}

	return ""
}
