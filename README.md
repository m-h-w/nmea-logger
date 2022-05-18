# nmea-logger
nmea 2000 datalogger for B&G instruments.

This repo contains the source code for an nmea data logger that is configured to work with a raspberry pi with an SK Pang PiCAN-M board (https://www.skpang.co.uk/products/pican-m-with-can-bus-micro-c-and-rs422-connector-3a-smps), tools to load data from the logger into a Mongo Atlas database, and a web frontend with associated APIs to analys the data from  the logger.

On the Raspberry Pi looger in the boat 
-------------------------------------
The pi code in this repo builds on (and is dependent upon) work done by Kees Verruijt. The source code for the required dependencies can be found here https://github.com/canboat/canboat. The tools used in this project are: candump2analyzer and analyzer, which intern work on the output of candump

The pi end of the logger is started at boot time by a shell script nmea-start.sh - this assumes that you have installed the can utils as described by the skpang.co.uk manual at the url above.

The *non-pi code* is divided int two sections, write and read, which respectively put data into mongoDB and read from it.

On the write side:
------------------

The tools/ directory contains command line front end to the various transformer ETL tools that operate on the output file from the Raspberry Pi Logger and transform them using files in /transform as follows:

*SailNjord - converst the output to a format that can be uploaded to the SailNjord website (https://www.sailnjord.com/). Not loaded into Mongo.
*Mongotranformer - converts the output to a mongo format and uploads to a MongoAtlas instance
*low-res-view - reads from a mongotransformer table and extracts the position data at 6 second intervals. This is to drive the fromt end map view.

The /mongodb dir contains the mongo drivers for accessing mongo Atlas.


On the read side
----------------
 
 The /frontend directory contains the react code that implements the ui

 The /api directory contains the code that power the UI.


More general detail on design ideas, thoughts and general musings can be found here: https://docs.google.com/document/d/1RJxxjj2bqD2BeqQbOhAEDa46bFF_WlRZTJlIrkAEbcc/edit?usp=sharing



