import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    - Reads all song files in JSON format into pandas dataframe
    - Insert all song-related records into the songs table in the PostgreSQL db
    - Insert all artist-related records into the artists table in the PostgreSQL db
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    - Reads all log files in JSON format into pandas dataframe
    - Insert all timestamps into the time table in the PostgreSQL db
    - Insert all user-related records into the users table in the PostgreSQL db
    - Insert songid and artistsid into the songplay_table in the PostgreSQL db
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page == 'NextSong', :]

    # convert timestamp column to datetime
    t = df.ts
    
    # insert time data records
    # time_data = 
    # column_labels = 
    time_df = pd.DataFrame({'start_time': pd.to_datetime(t).values,
                            'hour': pd.to_datetime(t).dt.hour.values,
                            'day': pd.to_datetime(t).dt.day.values,
                            'week': pd.to_datetime(t).dt.week.values,
                            'month': pd.to_datetime(t).dt.month.values,
                            'year': pd.to_datetime(t).dt.year.values,
                            'weekday': pd.to_datetime(t).dt.weekday.values})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    - Iterates through every single JSON file in both song folder and log folder
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    - Main function that calls process_data(), process_song_file(), and process_log_file()
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()