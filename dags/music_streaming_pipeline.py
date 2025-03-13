from datetime import datetime
import logging
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Constants
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")  
AWS_CONN_ID = "aws_conn"
REDSHIFT_CONN_ID = "redshift_conn"
RDS_CONN_ID = "aws_rds_connection"

# Task 1: Check if RDS has data
def check_rds_data():
    pg_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
    results = pg_hook.get_first("SELECT * FROM public.songs LIMIT 1;")
    return "fetch_data_from_RDS" if results else "stop_dag_no_rds_data"

# Task 2: Fetch data from RDS
def fetch_data_from_RDS(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
    results = pg_hook.get_records("SELECT * FROM public.songs;")
    kwargs['ti'].xcom_push(key="rds_data", value=results)

# Task 3: Check for S3 files
def check_for_s3_files(**kwargs):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = s3_hook.list_keys(S3_BUCKET_NAME)
    csv_keys = [k for k in keys if k.endswith('.csv')] if keys else []
    kwargs['ti'].xcom_push(key="csv_keys", value=csv_keys)
    return "validate_columns" if csv_keys else "stop_dag_no_s3_files"

# Task 4: Validate CSV Schema
def validate_data_columns(**kwargs):
    ti = kwargs['ti']
    keys = ti.xcom_pull(task_ids="check_s3_files", key="csv_keys")
    if not keys:
        return "stop_dag_invalid_columns"
    
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    valid_keys = []
    expected_columns = {'user_id', 'track_id', 'listen_time'}
    
    for key in keys:
        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET_NAME)
        data = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        if set(df.columns) == expected_columns:
            valid_keys.append(key)
    
    return "compute_kpis" if valid_keys else "stop_dag_invalid_columns"

# Task 5: Create KPI Table
def create_kpi_table():
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    sql = """
    CREATE TABLE IF NOT EXISTS Music_Streaming_KPIs (
        kpi_date DATE,
        kpi_hour INT,
        genre VARCHAR(255),
        listen_count INT,
        avg_track_duration FLOAT,
        popularity_index FLOAT,
        most_popular_track VARCHAR(255),
        unique_listeners INT,
        top_artist VARCHAR(255),
        track_diversity_index FLOAT
    );
    """
    hook.run(sql)

# Task 6: Compute KPIs
def compute_kpis(**kwargs):
    ti = kwargs['ti']
    rds_data = ti.xcom_pull(task_ids='fetch_data_from_RDS', key='rds_data')
    
    if not rds_data:
        return "stop_dag_no_rds_data"
    
    df = pd.DataFrame(rds_data, columns=['id', 'track_name', 'artist', 'genre', 'duration', 'popularity'])
    kpi_df = df.groupby('genre').agg(
        listen_count=('track_name', 'count'),
        avg_track_duration=('duration', 'mean'),
        popularity_index=('popularity', 'mean'),
        most_popular_track=('track_name', lambda x: x.value_counts().idxmax())
    ).reset_index()
    
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    for _, row in kpi_df.iterrows():
        sql = f"""
        INSERT INTO Music_Streaming_KPIs (kpi_date, kpi_hour, genre, listen_count, avg_track_duration, popularity_index, most_popular_track)
        VALUES (CURRENT_DATE, EXTRACT(HOUR FROM CURRENT_TIMESTAMP), '{row['genre']}', {row['listen_count']}, {row['avg_track_duration']}, {row['popularity_index']}, '{row['most_popular_track']}')
        """
        hook.run(sql)

# DAG Definition
with DAG(
    dag_id='music_streaming_kpi',
    start_date=datetime(2025, 3, 13),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    check_rds = BranchPythonOperator(
        task_id='check_rds_data',
        python_callable=check_rds_data
    )
    
    stop_dag_no_rds_data = EmptyOperator(task_id="stop_dag_no_rds_data")
    
    fetch_rds_data = PythonOperator(
        task_id='fetch_data_from_RDS',
        python_callable=fetch_data_from_RDS,
        provide_context=True
    )
    
    check_s3 = BranchPythonOperator(
        task_id='check_s3_files',
        python_callable=check_for_s3_files,
        provide_context=True
    )
    
    stop_dag_no_s3_files = EmptyOperator(task_id="stop_dag_no_s3_files")
    
    validate_s3 = BranchPythonOperator(
        task_id='validate_columns',
        python_callable=validate_data_columns,
        provide_context=True
    )
    
    stop_dag_invalid_columns = EmptyOperator(task_id="stop_dag_invalid_columns")
    
    create_kpi = PythonOperator(
        task_id='create_kpi_table',
        python_callable=create_kpi_table
    )
    
    compute_kpis_task = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
        provide_context=True
    )
    
    check_rds >> [fetch_rds_data, stop_dag_no_rds_data]
    fetch_rds_data >> check_s3
    check_s3 >> [validate_s3, stop_dag_no_s3_files]
    validate_s3 >> [compute_kpis_task, stop_dag_invalid_columns]
    compute_kpis_task >> create_kpi
