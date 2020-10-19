import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
#The following corrected the error:
#Can't adapt to type: numpy.int64
#This error came from writing to the db
from numpy import int64
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(int64, psycopg2._psycopg.AsIs)
#https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64


def process_song_file(cur, filepath):
    """
    Description: Reads a song json file and inserts the appropriate 
    records to sparkify's song and artist tables.

    Arguments:
        cur: the cursor object. 
        filepath: song json file path

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id',
                                     'year','duration']].iloc[0].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location',
                    'artist_latitude','artist_longitude']].iloc[0].values)
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: Reads a json log file and inserts the appropriate 
    records to sparkify's time, user, and songplay tables.

    Arguments:
        cur: the cursor object. 
        filepath: log json file path

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda x: pd.to_datetime(x,unit='ms'))
    
    # insert time data records
    time_data = (t,t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday )
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, 
                         row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: Walks through all subfolders of directory at filepath and records the filepaths of every
    json file found, then iterates through those files and applies a given 'func' to each file. 

    Arguments:
        cur: the cursor object. 
        conn: the connection object.
        filepath: starting file path for recursive searching.
        func: a function to be used on each file.

    Returns:
        None
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
    Description: Main function to call process_data function on two different 
    directories with two different parsing functions.

    Arguments:
        None
        
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()