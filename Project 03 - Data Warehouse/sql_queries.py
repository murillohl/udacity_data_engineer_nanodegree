import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration NUMERIC,
    sessionId INT,
    song TEXT,
    status INT,
    ts TIMESTAMP,
    userAgent TEXT,
    userId INT)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration NUMERIC,
    year INT)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL DISTKEY,
    level TEXT,
    song_id TEXT,
    artist_id TEXT,
    session_id INT,
    location TEXT,
    user_agent TEXT)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL PRIMARY KEY DISTKEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT NOT NULL PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year INT,
    duration NUMERIC)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude NUMERIC,
    longitude NUMERIC)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday TEXT)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON '{}'
timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(event.ts) as start_time,
                    event.userId AS user_id,
                    event.level AS level,
                    song.song_id AS song_id,
                    song.artist_id AS artist_id,
                    event.sessionId AS session_id,
                    event.location AS location,
                    event.userAgent AS user_agent
    FROM staging_events event
    JOIN staging_songs song
    ON (event.song = song.title AND event.artist = song.artist_name)
    WHERE event.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
    FROM staging_events
    WHERE (user_id IS NOT NULL 
    AND page = 'NextSong')
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id),
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id),
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(ts) AS start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(year FROM ts) AS year,
    EXTRACT(dayofweek FROM ts) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
