create_table_staging_reports = ("""
        CREATE TABLE IF NOT EXISTS public.staging_covid_19_daily_reports (
            "FIPS" int,
            "Admin2" varchar(255),
            "Province_State" varchar(255),
            "Country_Region" varchar(255),
            "Last_Update" DATE,
            "Lat" DECIMAL(12,8),
            "Lon_" DECIMAL(12,8),
            "Confirmed" int,
            "Deaths" int,
            "Recovered" int,
            "Active" int,
            "Combined_Key" varchar(500),
            "Incident_Rate" DECIMAL(26,19),
            "Case_Fatality_Ratio" DECIMAL(26,19)
        ) WITH (
        OIDS=FALSE
        );
    """)


create_table_time = ("""
        CREATE TABLE IF NOT EXISTS public."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
    """)

songplay_table_insert = ("""
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

time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
 extract(year from the_date) as year,
        extract(quarter from the_date) as quarter,
        extract(year from the_date) || '-' || extract(quarter from the_date) as year_quarter
    