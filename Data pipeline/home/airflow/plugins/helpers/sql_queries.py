class SqlQueries:
    truncuate_table=("""
    TRUNCUATE {}
    """ )
    songplay_table_insert = ("""
        INSERT INTO songplays (
            songplay_id,
            start_time,
            userid,
            level,
            song_id,
            artist_id,
            sessionid,
            location,
            useragent,
            start_time
        )
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)
    user_table_insert = ("""
        INSERT INTO users (
            userid,
            firstname,
            lastname,
            gender,
            level
        )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude    
        )
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)
    time_table_insert = ("""
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    copy_from_json_to_redshift=(
    """
    copy ''
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    format as json 'auto'
    reigon 'us-west-2'
    """
    )
     copy_from_json_with_json_path_to_redshift=(
    """
    copy ''
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    format as json 'auto'
    reigon 'us-west-2'
    json '{}'
    """
    )
    copy_from_json_to_redshift=(
    """
    copy ''
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    format as json 'auto'
    reigon 'us-west-2'
    """
    )
    copy_from_csv_to_redshift=(
    """
    copy ''
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    IGNOREHEADER 1
    DELIMITER ','
    reigon 'us-west-2'
    """
    )  
   count_of_nulls_in_songs_table = ("""
     SELECT count(*) as result
     FROM songs as s
     WHERE  s.song_id IS NULL
     
    """)
   count_of_nulls_in_artists_table = ("""
     SELECT count(*) as result
     FROM artists as a
     WHERE  a.artist_id IS NULL
  """)
    count_of_nulls_in_user_table = ("""
     SELECT count(*) as result
     FROM users as u
     WHERE  u.user_id IS NULL
  """)
    count_of_nulls_in_time_table = ("""
     SELECT count(*) as result
     FROM time as t
     WHERE NULL in (start_time, "hour", "month", "year", "day", "weekday")
  """)
     count_of_nulls_in_user_table = ("""
     SELECT count(*) as result
     FROM songplays as sp
     WHERE  sp.songplay_id IS NULL
  """)
    
    
    
   
    
    
    