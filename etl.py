import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import io

def process_song_file(cur, filepath):
    """ Description of Process Song File function
        This procedure takes a file from filepath, processes the information and adds it to the songs table.
        It then extracts the artist information from the file and adds that to the artist table.
        
        INPUTS:
        * cur the cursor variable for the database
        * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """ Description of Process Log File function
        This procedure takes a file from the given filepath and processes it.
        It extracts the user data and adds it to the user table.
        It then extracts time data and adds it to the time table.
        Finally it extracts the songplay data and adds it to the songplay table.
        
        INPUTS:
        * cur the cursor variable for the database
        * filepath the file path to the songplay file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list(zip(*(df['ts'],t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)))
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # create string and cvs like object for copying
    def csv_values(value) -> str:
        return str(value).replace('\n','\\n')
    csv_file_like_object = io.StringIO()
    
    cur.execute("select max(songplay_id) from songplays")
    last_index = cur.fetchone()[0]
    if last_index:
        last_index = last_index + 1
    else:
        last_index = 0
    
    # insert songplay records
    for index, row in df.iterrows():
        new_index = last_index + index
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        csv_file_like_object.write('|'.join(map(csv_values, (
            new_index,
            row.ts,
            row.userId,
            songid,
            artistid,
            row.sessionId,
            row.userAgent,
            row.level,
            row.location,
        ))) + '\n')
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, 'songplays', sep='|')


def process_data(cur, conn, filepath, func):
    """ Description of Process Data Function
        This procedure lists all files in the passed filepath and processes them individually using the passed function.
        
        INPUTS:
        * cur the cursor variable for the database
        * conn the connection variable to the database
        * filepath the file path to the songplay file
        * func the function to process the given file
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
    """ Description of Main Function
        This procedure creates a connection to the database, and a cursor for that connection.
        It then processes the data in the data folder with the corresponding functions.
        Those functions then add the data in the data folder to the database that is set up.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()