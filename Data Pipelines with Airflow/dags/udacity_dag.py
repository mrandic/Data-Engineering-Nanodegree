from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
REDSHIFT_CONN_ID    = 'redshift'
AWS_CREDENTIALS_ID  = 'aws_credentials'
S3_BUCKET           = 'udacity-dend'
S3_LOG_KEY          = 'log_data'
S3_SONG_KEY         = 'song_data'
REGION              = 'us-west-2'
LOG_JSON_PATH       = 'auto'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 5, 24),
}

dag = DAG(dag_id='udacity_dag',
          default_args      = default_args,
          description       = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *'
        )

# Start of Operator Section
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

stage_events_to_redshift    = StageToRedshiftOperator(
    task_id                 = 'Stage_events',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    aws_credentials_id      = AWS_CREDENTIALS_ID,
    table                   = 'public.staging_events',
    s3_bucket               = S3_BUCKET,
    s3_key                  = S3_LOG_KEY,
    region                  = REGION,
    truncate                = False
)

stage_songs_to_redshift     = StageToRedshiftOperator(
    task_id                 ='Stage_songs',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    aws_credentials_id      = AWS_CREDENTIALS_ID,
    table                   = 'public.staging_songs',
    s3_bucket               = S3_BUCKET,
    s3_key                  = S3_SONG_KEY,
    region                  = REGION,
    truncate                = False
)

load_songplays_table        = LoadFactOperator(
    task_id                 = 'Load_songplays_fact_table',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    sql                     = SqlQueries.songplay_table_insert,
    table                   = 'public.songplays',
    truncate                = False
)

load_user_dimension_table   = LoadDimensionOperator(
    task_id                 = 'Load_user_dim_table',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    sql                     = SqlQueries.user_table_insert,
    table                   = 'public.users',
    truncate                = True
)

load_song_dimension_table   = LoadDimensionOperator(
    task_id                 = 'Load_song_dim_table',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    sql                     = SqlQueries.song_table_insert,
    table                   = 'public.songs',
    truncate                = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id                 = 'Load_artist_dim_table',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    sql                     = SqlQueries.artist_table_insert,
    table                   = 'public.artists',
    truncate                = True
)

load_time_dimension_table   = LoadDimensionOperator(
    task_id                 = 'Load_time_dim_table',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    table                   = 'public.time',
    sql                     = SqlQueries.time_table_insert,
    truncate                = True
)

run_quality_checks          = DataQualityOperator(
    task_id                 = 'Run_data_quality_checks',
    dag                     = dag,
    redshift_conn_id        = REDSHIFT_CONN_ID,
    test_sql_array          = [ "SELECT COUNT(*) FROM songs WHERE songid IS NULL", \
                                "SELECT COUNT(*) FROM songs", \
                                "SELECT COUNT(*) FROM songplays", \
                                "SELECT COUNT(*) FROM artists", \
                                "SELECT COUNT(*) FROM artists", \
                                "SELECT COUNT(*) FROM time" \
                              ],
    expected_output         = [ lambda x: x == 0, \
                                lambda x: x >  0, \
                                lambda x: x >  0, \
                                lambda x: x >  0, \
                                lambda x: x >  0, \
                                lambda x: x >  0]
                            )

end_operator                = DummyOperator(task_id='Stop_execution',  dag=dag)
# End of Operator Section


# Ordering of tasks
start_operator              >> stage_events_to_redshift
start_operator              >> stage_songs_to_redshift

stage_events_to_redshift    >> load_songplays_table
stage_songs_to_redshift     >> load_songplays_table

load_songplays_table        >> load_user_dimension_table
load_songplays_table        >> load_song_dimension_table
load_songplays_table        >> load_artist_dimension_table
load_songplays_table        >> load_time_dimension_table

load_user_dimension_table   >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >> run_quality_checks

run_quality_checks          >> end_operator