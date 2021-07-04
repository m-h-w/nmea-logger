#!/bin/sh

candump can0|\
/home/pi/logger/exec/bin/candump2analyzer|\
/home/pi/logger/exec/bin/analyzer -nv|\
/home/pi/logger/exec/bin/logger 
