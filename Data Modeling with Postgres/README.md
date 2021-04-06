**Sparkify Postgres - Project Purpose and Goals**


The main objective is to create analytical database with SQL for music streaming service Sparklify.
Purpose of creating this database reflects the need from analytics team to understand the user nature of playing the songs in database (which songs, when, how long, etc.)
Since the data is stored in .json files (user activitiy on the app and list of the songs on user app) data engineer has a task to create a database schema supported with ETL pipeline for analysis.<br><br>



**Database Schema Design and ETL Pipeline**

Star schema has been used for database design in this project.
Central, fact table is the one consisting of all measurements on event songplays.
Other four tables (songs, artists, users and time) are referenced from the fact table with assigned primary keys on each table.

Data is stored in JSON files. The JSON files have been imported to pandas dataframes and then after being processed sent into database using psycopg2.

In order to clean and reduce dataset, songlplays were selected by applying filtering actions initiated from 'NextSong' page.
Also, timestamps were parsed from NIX time to datetime format.<br><br>


**Files used within the project**

1) *test.ipynb* - displays the first few rows of each table to check database.<br>
2) *create_tables.py* - drops and creates tables. This runs to reset tables before each time you run your ETL scripts.<br>
3) *etl.ipynb* - reads and processes a single file from song_data and log_data and loads the data into tables.<br>
4) *etl.py* - reads and processes files from song_data and log_data and loads them into tables.<br>
5) *sql_queries.py* - contains all sql queries, and is imported into the last three files above.<br><br>


**Steps for running the solution**

In order to execute this project, following files need to be run:
1) Run *create_tables.py* to create database and tables.<br>
2) Run *etl.py* from terminal to process and load data into the database.<br>
3) Run *test.ipynb* to confirm the creation of tables with the correct columns and validate results from executed ETL flow.<br>