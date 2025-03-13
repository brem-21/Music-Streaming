from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

def check_redshift_connection():
    """Checks the connection to Redshift and determines the next task."""
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn')
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        logging.info("✅ Connection to Redshift successful!")
        return "proceed_with_tasks"
    except Exception as e:
        logging.error(f"❌ Connection to Redshift failed: {e}")
        return "stop_dag"

with DAG(
    dag_id='redshift_connection_check',
    start_date=datetime(2025, 3, 13),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    check_connection = BranchPythonOperator(
        task_id='check_redshift_connection',
        python_callable=check_redshift_connection
    )

    stop_dag = EmptyOperator(task_id='stop_dag')
    proceed_with_tasks = EmptyOperator(task_id='proceed_with_tasks')

    check_connection >> [proceed_with_tasks, stop_dag]