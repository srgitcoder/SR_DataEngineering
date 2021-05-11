#!/usr/bin/env python

#File: eqdata.py
#Author: Steve Rezakhany
#Date of Design and Coding: Sunday, Monday, and Tuesday 5/9, 5/10, and 5/11/2021
#Coding Exercise Statement:
"""
       Coding Exercise - Tesla Motors Data Engineerign Group


Data Engineer exercise:
 
Please write an application in python language that calls the USGS API and store the result in a relational database of your choice.

https://earthquake.usgs.gov/fdsnws/event/1/


1) Please query all events that have occurred during year 2017

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


Thanks and Regards,
Srikanth Kotipalli.

"""


import json
import logging
import requests
import datetime
import sqlite3
import matplotlib.pyplot as plt
from requests.exceptions import HTTPError


logfilelogger = logging.getLogger(__name__)

con=sqlite3.connect('./earthquake.db')
cur = con.cursor()



def get_eqdata(year, month, begin, monthend):
    usgs_request_url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime='+year+'-'+month+'-'+begin+'&endtime='+year+'-'+month+'-'+monthend
    try:
        response = requests.get(usgs_request_url)

        # If successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        jresp = response.json()
        return jresp
    return False 


def plot_hour_hist(hour_list):
    plt.hist(hour_list, bins=24)
    plt.show()
    retun()


def find_max_mag():
    cur.execute("SELECT MAX(maxmag) FROM " + \
                    "(" + \
                        "SELECT MAX(mag) AS maxmag FROM janquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM febquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM marquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM aprquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM mayquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM junquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM julquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM augquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM sepquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM octquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM novquakes" + \ 
                        "UNION" +\
                        "SELECT MAX(mag) AS maxmag FROM decquakes" + \ 
                     ")")
    return cur.fetchone()[0] 



def get_bucket_hour_list(bucket_name):
    cur.execute("SELECT time FROM " + bucket_name + " ORDER BY time DESC ")
    return cur.fetchone() # return the whole list



def find_the_most_probable_hour(bucket_name):
    cur.execute("SELECT time, count(*) AS most_probable_time FROM " + bucket_name + " GROUP BY time ORDER BY most_probable_time DESC, time DESC LIMIT 1")
    return cur.fetchone()[0]



def inc_fetch_data(source_table_name):
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_01 FROM " + source_table_name + " AS st WHERE mag < 1")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_12 FROM " + source_table_name + " AS st WHERE mag >= 1" + \
            " AND mag < 2")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_23 FROM " + source_table_name + " AS st WHERE mag >= 2" + \
            " AND mag < 3")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_34 FROM " + source_table_name + " AS st WHERE mag >= 3" + \
            " AND mag < 4")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_45 FROM " + source_table_name + " AS st WHERE mag >= 4" + \
            " AND mag < 5")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_56 FROM " + source_table_name + " AS st WHERE mag >= 5" + \
            " AND mag < 6")
    cur.execute("SELECT mag, CAST(strftime('%H', datetime(st.time/1000, 'unixepoch')) AS INT) INTO mag_g6 FROM " + source_table_name + " AS st WHERE mag >= 6")



def fill_analysis_buckets():
    inc_fetch_data("janquakes")
    inc_fetch_data("febquakes")
    inc_fetch_data("marquakes")
    inc_fetch_data("aprquakes")
    inc_fetch_data("mayquakes")
    inc_fetch_data("junquakes")
    inc_fetch_data("julquakes")
    inc_fetch_data("augquakes")
    inc_fetch_data("sepquakes")
    inc_fetch_data("octquakes")
    inc_fetch_data("novquakes")
    inc_fetch_data("decquakes")
    return



def create_analysis_buckets():
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_01 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_12 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_23 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_34 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_45 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_56 (mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mag_g6 (mag real, time int)''')
    return



def create_tables():
    cur.execute('''CREATE TABLE IF NOT EXISTS janquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS febquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS marquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS aprquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS mayquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS junquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS julquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS augquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sepquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS octquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS novquakes (id text, mag real, time int)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS decquakes (id text, mag real, time int)''')
    return



def bulk_insert_into_eqdb(jdat, table_name):
    record_count = (jdat['metadata']['count'])
    print(record_count)
    insert_values_str = ""
    for eqeventcount in range(record_count):
        insert_values_str += "(" + "'" + str(jdat['features'][eqeventcount]['id']) + "'" + ", " +\
                str(jdat['features'][eqeventcount]['properties']['mag']) + ", " +\
                str(jdat['features'][eqeventcount]['properties']['time']) + ")"
        if eqeventcount < record_count - 1:
            insert_values_str += ","
        else:
            insert_values_str += ";"


    insert_query_str = "INSERT INTO " + table_name + " (id, mag, time) VALUES " 
    #print(insert_query_str)
    #print(insert_values_str)
    try:
        cur.execute(insert_query_str + insert_values_str) #executemany, place holder combo has a bug
    except sqlite3.Error as error:
        print("Failed to bulk insert multiple records into " + table_name + " table ", error)

    con.commit()



#*******************
# Driver code
#*******************

create_tables()


############# CAUTION: Only uncomment to get data from API and fill in the appropriate table
############# An example is shown 
'''
jeqdata = get_eqdata('2017', '03', '01', '31')
if jeqdata:
    bulk_insert_into_eqdb(jeqdata, "marquakes")
'''

############## Max mag analysis
maxmag = find_max_mag()
print("Maximum Magnitude is ", maxmag)

############## Incremental Fetch 
create_analysis_buckets()
fill_analysis_buckets()

############## Most Probable Hour analysis
mph = {}
mph['01'] = find_the_most_probable_hour("mag_01")
mph['12'] = find_the_most_probable_hour("mag_12")
mph['23'] = find_the_most_probable_hour("mag_23")
mph['34'] = find_the_most_probable_hour("mag_34")
mph['45'] = find_the_most_probable_hour("mag_45")
mph['56'] = find_the_most_probable_hour("mag_56")
mph['g6'] = find_the_most_probable_hour("mag_g6")

################ Plot the probable hour data histograms
# For example for 3-4 bucket
hour_list = get_bucket_hour_list("mag_34")
plot_hour_hist(hour_list)


con.close()


