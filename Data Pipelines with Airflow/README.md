# Data Pipelines with Airflow

## Project Outline
This project utilizes Airflow for building and scheduling data pipeline. 
It implements operators for staging data from S3 into Redshift, loading fact and dimensional tables and checks for data quality after data load.


## Database Schema

| Table name | Description |
| ---- | ---- |
| artists | artist name and location | 
| time | time-related info for timestamps |
| songplays | information on how songs were played | 
| users | information containing name, gender and level | 
| songs | information containing name, artist, year and duration | 

The project realizes a start schema with fact table `songplays` and dimensional tables `users`, `songs`, `artists`, and `time`.

## Project instructions
1. Specify AWS IAM role and database credentials in Airflow Connections.
2. Initiate Airflow with `/opt/airflow/start.sh` command.
3. Execute DAG in Airflow and monitor execution process.
4. Example of DAG and executon result is provided with `.png` files.