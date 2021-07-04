/* This programm reads from stdin and writes the output to either
 *  a local file or a cloud endpoint.
 *
 * when writing to a local file data is written to a file with the
 * following naming convention:
 *
 * nmea-log-file-<date>
 *
 * if the file already exists -<number> is appended to the filename.
 */

package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
	//"log"
	"strconv"
	//"io/ioutil"
)

var debug bool = false

// Information about File
type fileInfo struct {
	outputToFile bool // whether or not to use file
	fileOpen     bool // is there a file open
	fileHandle   *os.File
	fileName     string // name of open file
}

// Routing table - defines where the input stream gets sent
type router struct {
	file        fileInfo
	endpoint    bool
	googleDrive bool
}

func createFile(dst *router) error {
	currentTime := time.Now()

	// format date as YYYY-MM-DD
	filename := "/home/pi/logger/" + fmt.Sprintf(currentTime.Format(("2006-01-02")))
	baseFilename:= filename

	for i := 1; i < 100; i++ { // go around the loop until a filename that doesnt exist is found

		// Does filename already exist ?
		if _, err := os.Stat(filename); err == nil {
			// exists so add "-1", re-test, if still  exists add "-2" etc
			filename = baseFilename + "-" + strconv.Itoa(i)

			if debug == true {
				fmt.Println("filename iteration: " + filename)
			} 

		} else if os.IsNotExist(err) { //  filename does *not* exist, so OK to use

			if debug == true {
				fmt.Println("filename: " + filename)
			}

			// Open file with filename
			f, err := os.Create(filename)

			if err != nil {
				return err
			}

			// store file info in routing table
			dst.file.fileName = filename
			dst.file.fileHandle = f
			dst.file.fileOpen = true
			dst.file.outputToFile = true

			return nil

		} else { // Schrodinger: file may or may not exist. See err for details.
			return err
		}
	}

	//ToDo this is an error condition as would have run out of filenames if we get here
	return nil
}

func writeStdinToFile(outStream *bufio.Scanner, dst *router) error {

	//write to file
	var f *os.File = dst.file.fileHandle
	n, err := f.WriteString(outStream.Text()+"\n")

	if debug == true {
		fmt.Printf("wrote %d bytes to file: %s\r\n", n, dst.file.fileName)
	}
	return err
}

/*
routeStdIn()
------------
collect data from std and route it according to contents of routing table
contained in dst data object

v0.1 to a local file on the Pi

v0.2 exted to write to a cloud endpoint or gdrive or both...

*/

func routeStdIn(dst *router) error {

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		
		// write to file
		if dst.file.outputToFile == true {
			writeStdinToFile(scanner, dst)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// execution should reach here - need to add an error condition if it does
	return (nil)
}

func initFileWrite(dst *router) error {

	// find a filename that doesnt exist using todays date (YYYY-MM-DD) with a number appeended (upto 100)
	// for multiple files in the same day
	err := createFile(dst)

	if err != nil {
		return err // fatal error
	}

	if debug == true {

	fmt.Printf ("dst:  %+v\r\n",dst) // print out the conten of the routing table
	}

	return nil

}

func main() {

	// routing table
	var dst router

	// ToDo - sconfigure routing table depending on command line argumanets.

	// Initialise file to write to
	if err := initFileWrite(&dst); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	//  route stdin stream
	if err := routeStdIn(&dst); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}



	// clear up
	if dst.file.fileOpen == true {
		dst.file.fileHandle.Close() //close file
		dst.file.fileHandle.Sync()  // flush to nv storeage
	}

}
