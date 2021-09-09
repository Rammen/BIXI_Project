from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from upload_s3 import UploadToS3Operator

#Creation of the Dag and its default settings
default_args = {
    'owner': 'rammen',
    'start_date': datetime(2014, 1, 12),
    'depends_on_past': False
}

dag = DAG('BIXI_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None # Only when asked
        )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Upload the raw data into S3
upload_stations_s3 = UploadToS3Operator(
    task_id='Upload_Stations_S3',
    dag=dag,
    s3_connection="S3_conn",
    foldername="home/workspace/airflow/dags/raw_data/raw_stations/",
    bucketname="bixi-project-udacity", 
    replace=True)

upload_weather_s3 = UploadToS3Operator(
    task_id='Upload_Weather_S3',
    dag=dag,
    s3_connection="S3_conn",
    foldername="home/workspace/airflow/dags/raw_data/raw_weather/",
    bucketname="bixi-project-udacity", 
    replace=False)

upload_bixi_trips_s3 = UploadToS3Operator(
    task_id='Upload_BIXI_trips_S3',
    dag=dag,
    s3_connection="S3_conn",
    foldername="home/workspace/airflow/dags/raw_data/raw_bixi_trips/",
    bucketname="bixi-project-udacity", 
    replace=False)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag, trigger_rule='all_done')


# Order of the pipeline
start_operator >> [upload_stations_s3, upload_weather_s3, upload_bixi_trips_s3]
[upload_stations_s3, upload_weather_s3, upload_bixi_trips_s3] >> end_operator