## GENERAL OVERVIEW
The purpose of this project is to give the analsyts at Sparkify the ability to easily query and analyze Spotify information. It takes music information such as artists, songs, and the information on when it was played and adds it to a database for easy querying or reporting. 

## DATABASE DESIGN
The database design is rather simple. It consists of five tables and takes a star-schema type approach. The songs and artists are related to eachother and each point to the songplays table which contains the information on when the specific song was played.

![schema](/Schema.png)

## ETL
The ETL script reads all the files in data/song_data, splits it into songs and artists and adds the data to a posgresql database. It then takes all the log data in data/log_data and adds that information to the song_plays database using the copy command and closely mimics this article: https://hakibenita.com/fast-load-data-python-postgresql. Using this approach it's easy to add a new log file and process it with the below commands.

python create_tables.py
python etl.py

This will delete, and create new databases, then load the databases with the new data. This should be run whenever a new log or song file is added.