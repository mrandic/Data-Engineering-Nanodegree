## Project Outline
This project utilizes AWS Redshift for Data Warehouse creation. It consists of ETL pipeline, that extracts data from S3, transform the data within staging area and loads into dimensional tables for analytical purposes.

The data on S3 contains song and log information from a music store. This solution enables music stores to easily process loads of information efficiently.
This project handles data from multiple S3 buckets, making sure it can be analyzed easily and efficiently.

## Project Instructions
1. Setup a redshift cluster on AWS and provide connection parameters in `dwh.cfg`.
2. Create database schema by running `create_tables.py`.
3. Process the data from the configured S3 data sources by executing `etl.py`.
4. Run volume test check to confirm data is correctly loaded into Redshift `volume_test.py`.
5. Volume check result is visible under `volume_test.png` and AWS query execution status under `aws_queries.png`.

## Database Schema
| Table name | Description |
| ---- | ---- |
| artists | artist name and location | 
| time | time-related info for timestamps |
| songplays | information on how songs were played | 
| users | information containing name, gender and level | 
| songs | information containing name, artist, year and duration | 
| staging_events | staging table for event data |
| staging_songs | staging table for song data |

## ETL Pipeline
1. Extract song and log data from S3 buckets.
2. Transform data within staging area.
3. Load data into defined database schema.