# CAN_Data_Parser
Data processing program for CAN sensor data from EV battery pack experiment
<p>Current code is in /Branch/branch folder. It takes in a file name as a command line argument and processes the archived text file.
Text files for processing contain a timestamp, ID, and data byte array.
The parser adds data members to a MySQL database, intended for use in charting the data on the UPEL website.
