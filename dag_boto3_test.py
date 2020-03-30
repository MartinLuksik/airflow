from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import boto3


# default arguments for each task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG("s3_to_pandas",
          default_args=default_args,
          schedule_interval=None)  # "schedule_interval=None" means this dag will only be run by external commands

TEST_BUCKET = 'wireframe-synthetic-data'
TEST_KEY = 'vehicles.csv'

# simple download task
def s3_to_pandas(bucket, key, destination):
    s3 = boto3.resource('s3')
    obj = s3.get_object(Bucket='bucket', Key='key')
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df.head()

read_s3 = PythonOperator(
    task_id='s3_to_pandas',
    python_callable=s3_to_pandas,
    op_kwargs={'bucket': TEST_BUCKET, 'key': TEST_KEY},
    dag=dag)


sleep_task = BashOperator(
    task_id='sleep_for_60',
    bash_command='sleep 60',
    dag=dag)


read_s3.set_downstream(sleep_task)