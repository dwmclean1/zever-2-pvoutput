# zever-2-pvoutput
Collects data from Zever brand solar inverters and uploads the data to PVOutput.org

## Overview
Config.py holds all the neccessary info for collecting and uploading your solar data. Inverter IP address and request interval time in seconds can be passed in as arguments when the program is run, these will override what is in config.py.
All data is logged to 'database.csv' in the main directory. If the database does not exist it will be created when the program is run.
Inverter data and any relevant logging info is to the console.

## HOWTO
### Install dependencies
`$ pip install -r requirements.txt`
### Example usage
`$ python zever-2-pvoutput.py -ip 192.168.0.1 -interval 1000
