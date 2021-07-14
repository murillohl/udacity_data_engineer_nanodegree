import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read songs data in JSON format files, extract the fields neededs and insert them in
    Sparkfydb at Songs table and Artists table
    
    Args:
        cur (psycopg2.cursor()): The psycopg2 cursor
        filepath (str): The location of the song file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id','year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read logs data in JSON format files, extract the fields neededs and insert them in
    Sparkfydb at Users table, Time table, and then insert data in the facts table Songplays
    
    Args:
        cur (psycopg2.cursor()): The psycopg2 cursor
        filepath (str): The location of the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_df = df['ts'].to_frame()
    time_df['hour'] = time_df['ts'].dt.hour
    time_df['day'] = time_df['ts'].dt.day
    time_df['week'] = time_df['ts'].dt.week
    time_df['month'] = time_df['ts'].dt.month
    time_df['year'] = time_df['ts'].dt.year
    time_df['weekday'] = time_df['ts'].dt.weekday_name

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function to process all data files (songs and logs files)
    
    Args:
        cur (psycopg2.cursor()): The psycopg2 cursor
        conn (psycopg2.connection()): The database connection
        filepath (str): The location of files to be processed
        func (function): The function needed to process the specific data
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
    Function to extract data from song files and user activity logs in JSON format, transform them and
    load it to a PostgreSQL Database
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()