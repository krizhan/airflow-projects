from airflow import DAG
from airflow.operators.python_operator impoty PythonOperator
from airflow.operators.bash_operator impoty BashOperator
from airflow.operators.hive_operator impoty HiveOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import date, timedelta, datetime

import fetching_tweet
import cleaning_tweet

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('first_data_pipeline', start_date=datetime(2019, 02, 14), schedule_interval='@daily', default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
    # Create tasks 
    waiting_file_task = FileSensor(task_id='waiting_file_task', fs_conn_id='fs_default', filepath='./data.csv', poke_interval=5)
    fetching_tweet_task = PythonOperator(task_id='fetching_tweet_task', python_callable=fetching_tweet.main)
    cleaning_tweet_task = PythonOperator(task_id='cleaning_tweet_task', python_callable=cleaning_tweet.main)
    loading_into_hdfs_task = BashOperator(task_id='loading_into_hdfs_task', bash_command='hadoop fs -put -f /temp/data_cleaned.csv /tmp/')
    transfer_into_hive_task = HiveOperator(task_id='transfer_into_hive_task', hql="'"LOAD DATA INPAT 'tmp/data_cleaned.csv' INTO TABLE tweets PARTITION(dt='2018-10-01')")
    
    # Connect tasks into DAG
    waiting_file_task >> fetching_tweet_task >> cleaning_tweet_task >> loading_into_hdfs_task >> transfer_into_hive_task
