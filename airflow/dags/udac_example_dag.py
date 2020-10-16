from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
import pendulum
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "udacity",
    "start_date": datetime(2018, 11, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="{execution_date.year}/{execution_date.month}",
    jsonpath="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="public.songplays",
    select=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    table="public.users",
    select=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    table="public.songs",
    select=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    table="public.artists",
    select=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    table="public.time",
    select=SqlQueries.time_table_insert,
)

# Two example quality checks
quality_check_queries = [
    "select count(*) from public.songs where song_id is null",
    "select count(*) from public.artists where artist_id is null",
]

expected_results = [0, 0]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    queries=quality_check_queries,
    expected=expected_results,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
