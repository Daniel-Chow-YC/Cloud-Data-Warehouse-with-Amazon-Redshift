# Cloud Data Warehouse Project

## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. <br>
The aim of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 

## How To Run the Project
<p>
1) Go to the notebook `create_cluster.ipynb` and follow the steps in the notebook to create a redshift cluster as well as the required IAM role. 
<br>
After following the steps in the notebook, be sure to update the the config file dwh.cfg with the host and ARN.
<br>
2) Run the script **create_tables.py** to create the staging and analytics tables in the database <br>
`python create_tables.py`
<br>
3) Run the script **etl.py** to extract the data from the files in S3, load it in to the staging tables and then finally store it in the analytics tables. <br>
`python etl.py`
</p>

## Database scheme design

### Staging tables
- staging_events
- staging_songs

#### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong -
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
#### Dimension Tables
**users** - users in the app -
*user_id, first_name, last_name, gender, level*
<br>
**songs** - songs in music database - 
*song_id, title, artist_id, year, duration*
<br>
**artists** - artists in music database - 
*artist_id, name, location, lattitude, longitude*
<br>
**time** - timestamps of records in songplays broken down into specific units - 
*start_time, hour, day, week, month, year, weekday*
