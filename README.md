# nmea-logger
nmea 2000 datalogger for B&G instruments.

This repo contains the source code for an nmea data logger that is configured to work with a raspberry pi with an SK Pang PiCAN-M board (https://www.skpang.co.uk/products/pican-m-with-can-bus-micro-c-and-rs422-connector-3a-smps).

The repo is divided into two: the pi code and the server code which is designed to be run in AWS.

The pi code in this repo builds on (and is dependent upon) work done by Kees Verruijt. The source code for the required dependencies can be found here https://github.com/canboat/canboat. The tools used in this project are: candump2analyzer and analyzer, which intern work on the output of candump

The pi end of the logger is started at boot time by a shell script nmea-start.sh - this assumes that you have installed the can utils as described by the skpang.co.uk manual at the url above.


ToDo
-----
- Server Side 

1. Infrastructure and code to capture the data in AWS (probably in Dynamo and / or S3 )
2. Access control - using gmail oauth2
2. Feature extraction tools to find tacks and gybes
3. Calibration tools for calibrating boatspeed on both tacks and at different heel angles.


- Pi side 
----------
1. Adding the 4G modem
2. web server to host config and other local tools.

