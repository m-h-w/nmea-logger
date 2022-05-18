package api

import (
	"log"
	"net/http"

	apimongo "github.com/m-h-w/nmea-logger/api/dbdriver"
)

/*
Query structure
---------------
?timeframe=yymmdd - to return a whole tables worth of location points from the 6 second data table
?start=yymmddT00.00.00.000&stop=yymmddT00.00.00.000 - to return a time frame from the high res table


*/

func GetPosition(w http.ResponseWriter, r *http.Request) { // r is the request, w is the response

	// parse the request and extract the parameters.
	//https://pkg.go.dev/net/url#URL.Query

	q := r.URL.Query()

	//extract the paramenterss from the url Query string
	timeFrame := q.Get("timeframe")
	start := q.Get("start")
	stop := q.Get("stop")

	if timeFrame == "" { // this could be a precision fetch as opposed to a low res one

		if start == "" || stop == "" {
			w.WriteHeader(http.StatusBadRequest)
		} else {

			log.Printf("ToDo: Validate time delta and query high res table")
		}

	} else { // low res position fetch

		result, err := apimongo.GetLrPosition(timeFrame + "-six-second")
		if err == nil {

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK) // the order of w.WriteHeader matters. If you want to set the content type do it first.
			w.Write(result)

		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}
