from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from upload_s3 import UploadToS3Operator
from clean_weather import XmlToCsvOperator
from empty_table import EmptyTableOperator
from sql_queries import *

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

# Clean Weather
weather_XML_to_CSV = XmlToCsvOperator(
    task_id='Weather_XML_to_CSV',
    dag=dag,
    xml_folder='home/workspace/airflow/dags/raw_data/xml_weather/', 
    csv_folder='home/workspace/airflow/dags/raw_data/raw_weather/', 
    sub_document='./stationdata')

# Upload the raw data into S3
upload_stations_s3 = UploadToS3Operator(
    task_id='Upload_Stations_S3',
    dag=dag,
    s3_connection="S3_conn",
    foldername="home/workspace/airflow/dags/raw_data/raw_stations/",
    bucketname="bixi-project-udacity", 
    replace=False)
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

# Create empty table in Redshift (for fact & dimensions tables)
create_trips_empty = EmptyTableOperator(
    task_id='Create_trips_table_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    create_table_sql=sql_empty_trips)
create_stations_empty = EmptyTableOperator(
    task_id='Create_stations_table_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    create_table_sql=sql_empty_stations)
create_weather_empty = EmptyTableOperator(
    task_id='Create_weather_table_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    create_table_sql=sql_empty_weather)

# Dummy Operator to tell we completed the setup 
setup_complete_operator = DummyOperator(task_id='setup_completed',  dag=dag, trigger_rule='all_done')

# Define the order of the pipeline
start_operator >> weather_XML_to_CSV >> upload_weather_s3 >> create_weather_empty >> setup_complete_operator
start_operator >> upload_bixi_trips_s3 >> create_trips_empty >> setup_complete_operator
start_operator >> upload_stations_s3 >> create_stations_empty >> setup_complete_operator
