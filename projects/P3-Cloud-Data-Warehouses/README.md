# Project 2: Data Warehouse

<p align="center"><img src="images/redshift.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

## Introduction
We are responsible for creating an Data Warehouse with Redshift cluster for the analytics team at Sparkify. They were interested in analyzing data collected on songs and user activity through their new music streaming app. Currently, the data is in JSON files, we should query the data on large amount data set.
We will use python to do Infrastructure as Code, to create Redshift Cluster and put data into cluster via program. Once warehouse is complete we should be able to delete clusrer via python script. 


## Available Data
### Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Example:
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset
The log dataset consists of logs of activity of users. 

Example:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
<img src="./images/log-data.png" width="50%"/>

# Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

## Table Schemas
<img src="./images/StarSchema.png" width="50%"/>
<img src="./images/StagingTable.png" width="50%"/>

#### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page `NextSong`
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    * songplay_id is Primary in this table and we have start_time and user_id as refrennce to time and users table respectively 

#### Dimension Tables
2. <b>users</b> - users in the app
    * `user_id, first_name, last_name, gender, level`
    * user_id is a Primary key.
3. <b>songs</b> - songs in music database
    * `song_id, title, artist_id, year, duration`
    * sonng_id is Primary Key
4. <b>artists</b> - artists in music database
    * `artist_id, name, location, lattitude, longitude`
    * artist_id is a Primary Key
5. <b>time</b> - timestamps of records in <b>songplays</b> broken down into specific units
    * `start_time, hour, day, week, month, year, weekday`
    * start_time is a Primary Key