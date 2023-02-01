import sys
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/home/christian/Desktop/pintrest_data_processing_pipeline")
import spark_cassandra.s3_to_spark_to_cassandra as sc

spark_job = sc.Spark_DAG()

default_args = {
    'owner': 'Christian',
    'depends_on_past': False,
    'email': ['chris.emmanuel@hotmail.com'],
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1,
    'start_date': datetime(2022, 9, 14), 
    'retry_delay': timedelta(minutes=1), 
    'end_date': datetime(2022, 10, 14)
}

with DAG(dag_id='spark_dag', 
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['spark']
    ) as dag:

    spark_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=spark_job.run_s3_to_spark_to_cassandra,
    )