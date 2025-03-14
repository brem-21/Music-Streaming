from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import logging
from io import StringIO  # Import StringIO for JSON handling

# Constants
REDSHIFT_CONN_ID = "redshift_conn"
RDS_CONN_ID = "aws_rds_connection"
S3_BUCKET = Variable.get("S3_BUCKET_NAME")
AWS_RDS_CONNECTION = "aws_rds_connection"
AWS_CONN_ID = "aws_conn"
S3_ARCHIVE_FOLDER = "archive"
BATCH_SIZE = 1000  # Define batch size for extraction

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'Music_Streaming_ETL_Pipeline',
    default_args=default_args,
    description='ETL Pipeline for Music Data with Batched Extraction',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Check for data in RDS database (users and songs tables)
def check_rds_data(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
        users_count = hook.get_first("SELECT COUNT(*) FROM users")[0]
        songs_count = hook.get_first("SELECT COUNT(*) FROM songs")[0]
        
        if users_count == 0 or songs_count == 0:
            logging.info("No data in RDS database. Ending DAG.")
            return "end_dag"
        else:
            logging.info("Data found in RDS database. Proceeding to fetch data.")
            return "fetch_rds_users"
    except Exception as e:
        logging.error(f"Error checking RDS data: {e}")
        raise

check_rds_data_task = BranchPythonOperator(
    task_id='check_rds_data',
    python_callable=check_rds_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Fetch users data from RDS in batches
def fetch_rds_users(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
        offset = 0
        users_df = pd.DataFrame()
        
        while True:
            batch = hook.get_pandas_df(
                f"SELECT * FROM users ORDER BY user_id LIMIT {BATCH_SIZE} OFFSET {offset}"
            )
            if batch.empty:
                break
            users_df = pd.concat([users_df, batch], ignore_index=True)
            offset += BATCH_SIZE
        
        kwargs['ti'].xcom_push(key='users_df', value=users_df.to_json(orient='split'))
        logging.info("Fetched users data from RDS in batches.")
    except Exception as e:
        logging.error(f"Error fetching users data from RDS: {e}")
        raise

fetch_rds_users_task = PythonOperator(
    task_id='fetch_rds_users',
    python_callable=fetch_rds_users,
    provide_context=True,
    dag=dag,
)

# Task 3: Fetch songs data from RDS in batches
def fetch_rds_songs(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
        offset = 0
        songs_df = pd.DataFrame()
        
        while True:
            batch = hook.get_pandas_df(
                f"SELECT * FROM songs ORDER BY track_id LIMIT {BATCH_SIZE} OFFSET {offset}"
            )
            if batch.empty:
                break
            songs_df = pd.concat([songs_df, batch], ignore_index=True)
            offset += BATCH_SIZE
        
        kwargs['ti'].xcom_push(key='songs_df', value=songs_df.to_json(orient='split'))
        logging.info("Fetched songs data from RDS in batches.")
    except Exception as e:
        logging.error(f"Error fetching songs data from RDS: {e}")
        raise

fetch_rds_songs_task = PythonOperator(
    task_id='fetch_rds_songs',
    python_callable=fetch_rds_songs,
    provide_context=True,
    dag=dag,
)

# Task 4: Check for files in S3 bucket
def check_s3_files(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        files = s3_hook.list_keys(bucket_name=S3_BUCKET)
        
        if not files:
            logging.info("No files in S3 bucket. Ending DAG.")
            return "end_dag"
        else:
            logging.info("Files found in S3 bucket. Proceeding to fetch data.")
            return "fetch_s3_data"
    except Exception as e:
        logging.error(f"Error checking S3 files: {e}")
        raise

check_s3_files_task = BranchPythonOperator(
    task_id='check_s3_files',
    python_callable=check_s3_files,
    provide_context=True,
    dag=dag,
)

# Task 5: Fetch S3 data in batches
def fetch_s3_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        files = s3_hook.list_keys(bucket_name=S3_BUCKET)
        s3_data = []
        
        for file in files:
            obj = s3_hook.get_key(file, bucket_name=S3_BUCKET)
            df = pd.read_csv(obj.get()['Body'])
            s3_data.append(df)
        
        # Concatenate all dataframes
        s3_df = pd.concat(s3_data, ignore_index=True)  # Reset index to ensure uniqueness
        
        # Push the dataframe to XCom as JSON
        kwargs['ti'].xcom_push(key='s3_df', value=s3_df.to_json(orient='split'))  # Use 'split' orient for non-unique indexes
        logging.info("Fetched S3 data in batches.")
    except Exception as e:
        logging.error(f"Error fetching S3 data: {e}")
        raise

fetch_s3_data_task = PythonOperator(
    task_id='fetch_s3_data',
    python_callable=fetch_s3_data,
    provide_context=True,
    dag=dag,
)

# Task 6: Merge RDS and S3 data into one dataframe
def merge_data(**kwargs):
    try:
        users_json = kwargs['ti'].xcom_pull(task_ids='fetch_rds_users', key='users_df')
        songs_json = kwargs['ti'].xcom_pull(task_ids='fetch_rds_songs', key='songs_df')
        s3_json = kwargs['ti'].xcom_pull(task_ids='fetch_s3_data', key='s3_df')
        
        users_df = pd.read_json(StringIO(users_json), orient='split')
        songs_df = pd.read_json(StringIO(songs_json), orient='split')
        s3_df = pd.read_json(StringIO(s3_json), orient='split')
        
        # Log column names to help diagnose issues
        logging.info(f"users_df columns: {users_df.columns.tolist()}")
        logging.info(f"songs_df columns: {songs_df.columns.tolist()}")
        logging.info(f"s3_df columns: {s3_df.columns.tolist()}")
        
        # Fix: We need to first merge s3_df with songs_df on track_id
        # and then merge the result with users_df
        
        # First merge: s3_df with songs_df on track_id
        intermediate_df = pd.merge(songs_df, s3_df, on='track_id', how='inner')
        
        # Second merge: result with users_df
        # The S3 data likely contains user interactions, so it should have user_id
        if 'user_id' in intermediate_df.columns:
            merged_df = pd.merge(intermediate_df, users_df, on='user_id', how='inner')
        else:
            # If user_id is not in intermediate_df, we need to use a different approach
            # Assuming s3_df contains user_id for joins
            merged_df = pd.merge(users_df, intermediate_df, left_on='user_id', right_on='user_id', how='inner')
        
        kwargs['ti'].xcom_push(key='merged_df', value=merged_df.to_json(orient='split'))
        logging.info("Merged RDS and S3 data successfully.")
    except Exception as e:
        logging.error(f"Error merging data: {e}")
        raise

merge_data_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    provide_context=True,
    dag=dag,
)

# Task 7: Compute KPIs
def compute_kpis(**kwargs):
    try:
        merged_df = pd.read_json(kwargs['ti'].xcom_pull(task_ids='merge_data', key='merged_df'), orient='split')
        
        # Daily Genre-Level KPIs
        genre_kpis = merged_df.groupby('track_genre').agg(
            listen_count=('play_count', 'sum'),
            average_track_duration=('duration', 'mean'),
            popularity_index=('play_count', 'sum') + ('likes', 'sum') + ('shares', 'sum'),
            most_popular_track=('track_id', lambda x: x.value_counts().idxmax())
        ).reset_index()
        
        # Hourly KPIs
        merged_df['hour'] = pd.to_datetime(merged_df['listen_time']).dt.hour
        hourly_kpis = merged_df.groupby('hour').agg(
            unique_listeners=('user_id', 'nunique'),
            top_artists=('artist_id', lambda x: x.value_counts().idxmax()),
            track_diversity_index=('track_id', lambda x: x.nunique() / x.count())
        ).reset_index()
        
        kwargs['ti'].xcom_push(key='genre_kpis', value=genre_kpis.to_json(orient='split'))
        kwargs['ti'].xcom_push(key='hourly_kpis', value=hourly_kpis.to_json(orient='split'))
        logging.info("Computed KPIs.")
    except Exception as e:
        logging.error(f"Error computing KPIs: {e}")
        raise

compute_kpis_task = PythonOperator(
    task_id='compute_kpis',
    python_callable=compute_kpis,
    provide_context=True,
    dag=dag,
)

# Task 8: Create KPI table in Redshift
def create_kpi_table_in_redshift(**kwargs):
    """
    Create the KPI table in Amazon Redshift if it doesn't exist.
    """
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS music_kpi_metrics (
            track_genre VARCHAR(255),
            listen_count INT,
            avg_track_duration_sec FLOAT,
            popularity_index FLOAT,
            most_popular_track VARCHAR(255),
            stream_hour INT,
            unique_listeners INT,
            top_artists VARCHAR(255),
            track_diversity_index FLOAT,
            kpi_type VARCHAR(50)
        );
        """
        
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        redshift_hook.run(create_table_query)
        logging.info("Table `music_kpi_metrics` is ready in Redshift.")
    except Exception as e:
        logging.error(f"Error creating KPI table in Redshift: {e}")
        raise

create_kpi_table_task = PythonOperator(
    task_id='create_kpi_table_in_redshift',
    python_callable=create_kpi_table_in_redshift,
    provide_context=True,
    dag=dag,
)

# Task 9: Load genre KPIs into Redshift using SQL
load_genre_kpis_task = PostgresOperator(
    task_id='load_genre_kpis',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
    INSERT INTO music_kpi_metrics (track_genre, listen_count, avg_track_duration_sec, popularity_index, most_popular_track, kpi_type)
    VALUES (%s, %s, %s, %s, %s, 'genre');
    """,
    parameters=[
        ('{{ ti.xcom_pull(task_ids="compute_kpis", key="genre_kpis")["genre"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="genre_kpis")["listen_count"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="genre_kpis")["average_track_duration"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="genre_kpis")["popularity_index"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="genre_kpis")["most_popular_track"] }}')
    ],
    dag=dag,
)

# Task 10: Load hourly KPIs into Redshift using SQL
load_hourly_kpis_task = PostgresOperator(
    task_id='load_hourly_kpis',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
    INSERT INTO music_kpi_metrics (stream_hour, unique_listeners, top_artists, track_diversity_index, kpi_type)
    VALUES (%s, %s, %s, %s, 'hourly');
    """,
    parameters=[
        ('{{ ti.xcom_pull(task_ids="compute_kpis", key="hourly_kpis")["hour"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="hourly_kpis")["unique_listeners"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="hourly_kpis")["top_artists"] }}',
         '{{ ti.xcom_pull(task_ids="compute_kpis", key="hourly_kpis")["track_diversity_index"] }}')
    ],
    dag=dag,
)

# Task 11: Archive data
def archive_data(**kwargs):
    try:
        # Archive S3 files
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        files = s3_hook.list_keys(bucket_name=S3_BUCKET)
        
        for file in files:
            new_key = f"{S3_ARCHIVE_FOLDER}/{file.split('/')[-1]}"
            s3_hook.copy_object(source_bucket_name=S3_BUCKET, dest_bucket_name=S3_BUCKET, source_bucket_key=file, dest_bucket_key=new_key)
            s3_hook.delete_objects(bucket=S3_BUCKET, keys=file)
        
        logging.info("Data archived successfully.")
    except Exception as e:
        logging.error(f"Error archiving data: {e}")
        raise

archive_data_task = PythonOperator(
    task_id='archive_data',
    python_callable=archive_data,
    provide_context=True,
    dag=dag,
)

# Dummy operator to end the DAG
end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Define task dependencies
check_rds_data_task >> [fetch_rds_users_task, end_dag]
fetch_rds_users_task >> fetch_rds_songs_task
fetch_rds_songs_task >> merge_data_task

check_s3_files_task >> [fetch_s3_data_task, end_dag]
fetch_s3_data_task >> merge_data_task

merge_data_task >> create_kpi_table_task >> compute_kpis_task >> [load_genre_kpis_task, load_hourly_kpis_task] >> archive_data_task >> end_dag