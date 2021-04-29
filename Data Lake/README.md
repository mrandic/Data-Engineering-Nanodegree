# Data Lake with Spark

## Project Outline
This project utilizes AWS EMR for Data Lake creation. It consists of ETL pipeline, that extracts data residing in S3 JSON logs, transform the data into dimensional tables with Spark and loads back to S3.
The aim of this data lake is to help analysts in Sparkify find useful insights on how users use their service and music they are listening to.

## Data Lake Schema

| Table name | Description |
| ---- | ---- |
| artists | artist name and location | 
| time | time-related info for timestamps |
| songplays | information on how songs were played | 
| users | information containing name, gender and level | 
| songs | information containing name, artist, year and duration | 

The project realizes a start schema with fact table `songplays` and dimensional tables `users`, `songs`, `artists`, and `time`.

## Project instructions
1. Define AWS IAM Credentials in `dl.cfg`
2. Define output data path in `etl.py`.
3. Execute `etl.py`.