from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,LoadFactOperator,LoadDimensionOperator,DataQualityOperator,StageToRedshiftOperator)
from helpers import SqlQueries
AWS_KEY = os.environ.get('###')
AWS_SECRET = os.environ.get('####')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 11, 1),
    'depends_on_past': False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catch_up':False
   
}

dag = DAG('Sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    source_file="s3://udacity-dend/log_data",
    target_table="staging_events",
    file_type="json",
    json_path="s3://udacity-dend/log_json_path.json"
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    source_file="s3://udacity-dend/log_data",
    target_table="staging_songs",
    file_type="json"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=Dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    target_table="songplays",
    
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    target_table="users",
    
    
    
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    reshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    target_table='songs'
    
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    target_table="artist"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    target_table="time"
    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    reshift_conn_id="redshift",
    sql_queries=[
        (SqlQuery.count_of_nulls_in_songs_table,0),
        (SqlQuery.count_of_nulls_in_users_table,0),
        (SqlQuery.count_of_nulls_in_artists_table,0),
        (SqlQuery.count_of_nulls_in_time_table,0),
        (SqlQuery.count_of_nulls_in_songplays_table,0)]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator