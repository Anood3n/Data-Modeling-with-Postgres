import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    -Reads from the song dataset 
    -Extract its data into panda dataframe 
    -Insert the values from the data frame into the song and artist tables, respectively.

            Parameters:
                    cur : a cursor for PostgreSQL connection. 
                    filepath: an object of the path where the song dataset is located.

            Returns:
                    None
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df [["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name","artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    -Reads from the log dataset 
    -Filter the logs dataset to include only the users that their activity level is == 'NextSong'
    -Convert the timestamp field type "ts" to datetime for the time table , extract its values into a list then convert it to panda dataframe  
    -Insert the values from the dataframe into the time table 
    -Insert the selected columns into the user table 
    - Run the the select query to get songid and artistid from song and artist tables,convert "ts" column to datetime and insert its values into the songplay table.

            Parameters:
                    cur : a cursor for PostgreSQL connection. 
                    filepath: an object of the path where the song dataset is located.

            Returns:
                    None
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName','gender','level']]

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
            
    # convert ts coulmn to datetime and set it to timestamp
    timestamp = pd.to_datetime(row.ts,unit='ms')

    # insert songplay record
    songplay_data = (timestamp, row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
    cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    '''
    -a loop to iterate over all the files found in the path extension to get processed   
  
            Parameters:
                    cur : a cursor for PostgreSQL connection.
                    conn: PostgreSQL connection
                    filepath: an object of the path where the datasets are located.
                    func: a function to call and process the datasets files in the given filepath

            Returns:
                    None
    '''
    
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    
if __name__ == "__main__":
        main()