# nmea-logger
nmea 2000 datalogger for B&G instruments.

This repo contains the source code for an nmea data logger that is configured to work with a raspberry pi with an SK Pang PiCAN-M board (https://www.skpang.co.uk/products/pican-m-with-can-bus-micro-c-and-rs422-connector-3a-smps).

The repo is divided into two: the pi code and the server code which is designed to be run in AWS.

The pi code in this repo builds on (and is dependent upon) work done by Kees Verruijt. The source code for the required dependencies can be found here https://github.com/canboat/canboat. The tools used in this project are: candump2analyzer and analyzer, which intern work on the output of candump

The pi end of the logger is started at boot time by a shell script nmea-start.sh - this assumes that you have installed the can utils as described by the skpang.co.uk manual at the url above.

The tools/ directory contains command line tools for analysing the data output from the nmea-logger and writing to the Mongo Atlas time series DB


ToDo
-----
- Server Side. more detail here: https://docs.google.com/document/d/1RJxxjj2bqD2BeqQbOhAEDa46bFF_WlRZTJlIrkAEbcc/edit?usp=sharing

1. add the test tools to write to Mongo DB
2. write a secure API for the web client
3. extend tool to extract all of the data fields in the nmea logger output.

Web Client
----------

1. write a react web app to read data from a local file and display as a grahp on a web page using D3
2. build login capability and read data from the API

- Pi side 
----------
1. Adding the 4G modem
2. web server to host config and other local tools.

