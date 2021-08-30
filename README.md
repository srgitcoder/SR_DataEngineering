# SR_DataEngineering
A python language application that calls the USGS API and stores the result in a relational database.

https://earthquake.usgs.gov/fdsnws/event/1/


1) All events that have occurred during year 2017 are qurried

2) Read a JSON response from the API

3) Design the database objects required to store the result in a relational fashion.

4) Store the response in those objects

5) Add incremental fetch design to python script

6) Provide query/analysis to give biggest earthquake of 2017

7) Provide query/analysis to give most probable hour of the day for the earthquakes bucketed by the range of magnitude (0-1,1-2,2-3,3-4,4-5,5-6,>6   For border values in the bucket, include them in the bucket where the value is a lower limit so for 1 include it in 1-2 bucket)

For question 1-5 provide following

    Code for the integration
    DB schema

For question 6 and 7 provide following

    Queries for the analysis
    Any interesting visualization (using any open source product or trial version of any product) for these questions. Just attach screenshot.
    
