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

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionid int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userID int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs int,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
    (user_id int PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar, 
    level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
    (song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar, 
    year int, 
    duration numeric);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
    (artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log_data'
                            credentials 'aws_iam_role={}'
                             compupdate off region 'us-west-2'
                             timeformat as 'epochmillisecs'
                             truncatecolumns blanksasnull emptyasnull
                             json 's3://udacity-dend/log_json_path.json'  ;
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""COPY staging_songs FROM 's3://udacity-dend/song_data/A/A/A'
                            credentials 'aws_iam_role={}'
                            format as json 'auto' compupdate off region 'us-west-2';
""").format(*config['IAM_ROLE'].values())



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' AS start_time, e.userID AS user_id, e.level, s.song_id, s.artist_id, e.sessionid AS session_id, e.location, e.userAgent AS user_agent FROM staging_events AS e 
    INNER JOIN staging_songs AS s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id, firstName as first_name, lastName AS last_name, gender, level FROM staging_events
    WHERE page = 'NextSong' AND user_id NOT IN (SELECT DISTINCT user_id FROM users);
""")
    

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts.start_time,
    EXTRACT (HOUR FROM ts.start_time), EXTRACT (DAY FROM ts.start_time),
    EXTRACT (WEEK FROM ts.start_time), EXTRACT (MONTH FROM ts.start_time),
    EXTRACT (YEAR FROM ts.start_time), EXTRACT (WEEKDAY FROM ts.start_time) FROM
    (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) ts
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
#copy_table_queries = [staging_events_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
