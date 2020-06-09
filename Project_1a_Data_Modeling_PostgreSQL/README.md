## Background
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Goal

The goal of this project is to design an analytic friendly database using the Star-Schema and to build an ETL pipeline that stores all the JSON format data into this Star-Schema database.

## Database Schema Design (Star-Schema)

Fact Table: 
    1. Songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    
Dimensional Tables:
    1. Users (user_id, first_name, last_name, gender, level) 
    2. Songs (song_id, title, artist_id, year, duration) 
    3. Artists (artist_id, name, location, latitude, longitude) 
    4. Time (start_time, hour, day, week, month, year, weekday) 

## Files

sql_queries.py: This file stores queries for dropping tables, creating tables, inserting values to tables, and finding IDs.

create_tables.py: This file contains functions that execute queries in sql_queries.py file to actually create tables in database.

etl.py: This file contains script that extracts data from JSON database, transform data, and then load data into the Star-Schema database.

## How To Guide

We first need to ensure that all the queries are properly written in the sql_queries.py file, then we need to run the create_tables.py file, and finally, run the etl.py file.