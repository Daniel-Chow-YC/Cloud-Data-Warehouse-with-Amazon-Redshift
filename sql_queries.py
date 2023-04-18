import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
( 
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT, 
    level VARCHAR,
    location VARCHAR,
    method VARCHAR, 
    page VARCHAR, 
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status VARCHAR,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT, 
    year VARCHAR
)
""")

# CREATE FINAL TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL distkey,
user_id VARCHAR(10) NOT NULL, 
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR, 
session_id VARCHAR, 
location VARCHAR,
user_agent VARCHAR
) 
sortkey(start_time, song_id) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id VARCHAR NOT NULL PRIMARY KEY sortkey distkey, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR,
level VARCHAR
) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR NOT NULL UNIQUE PRIMARY KEY sortkey distkey, 
title VARCHAR, 
artist_id VARCHAR, 
year VARCHAR, 
duration FLOAT
) 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR NOT NULL UNIQUE PRIMARY KEY sortkey distkey,
name VARCHAR, 
location VARCHAR, 
latitude FLOAT, 
longitude FLOAT
) 
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP NOT NULL UNIQUE PRIMARY KEY sortkey, 
hour VARCHAR, 
day VARCHAR, 
week VARCHAR, 
month VARCHAR,
year VARCHAR, 
weekday VARCHAR
) 
diststyle all 
""")

# LOAD DATA FROM S3 INTO STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
credentials 'aws_iam_role={}'
JSON {} 
compupdate off region 'us-west-2'
timeformat as 'epochmillisecs';
""").format(config.get('S3','LOG_DATA') , config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {} 
credentials 'aws_iam_role={}'
JSON 'auto' 
compupdate off region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# LOAD DATA FROM STAGING TABLE INTO FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT 
    DISTINCT e.ts, 
    e.userId, 
    e.level,
    s.song_id,
    s.artist_id, 
    e.sessionId,
    e.location,
    e.userAgent
FROM staging_events e
LEFT JOIN staging_songs s 
ON (e.song = s.title AND e.artist = s.artist_name)
WHERE e.page = 'NextSong' 
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE userId IS NOT NULL 
ORDER BY userId DESC 
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude) 
SELECT 
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
ORDER BY artist_id DESC
""")

time_table_insert = ("""
INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
SELECT 
DISTINCT start_time, 
EXTRACT(HOUR from start_time),
EXTRACT(DAY from start_time),
EXTRACT(WEEK from start_time),
EXTRACT(MONTH from start_time),
EXTRACT(YEAR from start_time),
EXTRACT(DAYOFWEEK from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
