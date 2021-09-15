from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from upload_s3 import UploadToS3Operator
from clean_weather import XmlToCsvOperator
from empty_table import EmptyTableOperator
from stage_redshift import StageToRedshiftOperator
from drop_table import DropTableOperator
from transform_tables import TransformDataOperator
from sql_queries import *


"""
Setup the DAGS and the BIXI pipeline 
"""

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


"""
This sections work on the setup of the data prior the ETL:
1. Clean data and change file type (if necessary)
2. Load raw data into S3
3. Create empty tables in Redshift if they do not exists
"""

# start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Clean Weather
# weather_XML_to_CSV = XmlToCsvOperator(
#     task_id='Weather_XML_to_CSV',
#     dag=dag,
#     xml_folder='home/workspace/airflow/dags/raw_data/xml_weather/', 
#     csv_folder='home/workspace/airflow/dags/raw_data/raw_weather/', 
#     sub_document='./stationdata')

# Upload the raw data into S3
# upload_stations_s3 = UploadToS3Operator(
#     task_id='Upload_Stations_S3',
#     dag=dag,
#     s3_connection="S3_conn",
#     foldername="home/workspace/airflow/dags/raw_data/raw_stations/",
#     bucketname="bixi-project-udacity", 
#     replace=False)
# upload_weather_s3 = UploadToS3Operator(
#     task_id='Upload_Weather_S3',
#     dag=dag,
#     s3_connection="S3_conn",
#     foldername="home/workspace/airflow/dags/raw_data/raw_weather/",
#     bucketname="bixi-project-udacity", 
#     replace=False)
# upload_bixi_trips_s3 = UploadToS3Operator(
#     task_id='Upload_BIXI_trips_S3',
#     dag=dag,
#     s3_connection="S3_conn",
#     foldername="home/workspace/airflow/dags/raw_data/raw_bixi_trips/",
#     bucketname="bixi-project-udacity", 
#     replace=False)

# Create empty table in Redshift (for fact & dimensions tables)
# create_trips_empty = EmptyTableOperator(
#     task_id='Create_trips_table_Redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     create_table_sql=sql_empty_trips)
# create_stations_empty = EmptyTableOperator(
#     task_id='Create_stations_table_Redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     create_table_sql=sql_empty_stations)
# create_weather_empty = EmptyTableOperator(
#     task_id='Create_weather_table_Redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     create_table_sql=sql_empty_weather)

# Dummy Operator to tell we completed the setup 
setup_complete_operator = DummyOperator(task_id='setup_completed',  dag=dag, trigger_rule='all_done')

"""
Extract-transform-load (ETL) pipeline:
- Create staging table in redshift for the data and load the raw data 
- Transform the raw data into fact and dimensions tables
"""

#Bixi trips pipeline
# create_trips_staging = EmptyTableOperator(
#     task_id='Create_trips_StagingTable_Redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     create_table_sql=sql_trips_staging)
# fill_staging_trips = StageToRedshiftOperator(
#     task_id='fill_staging_trips',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     target_table="public.trips_staging",
#     columns_sql="start_date, start_station_code, end_date, end_station_code, duration_sec, is_member",
#     s3_bucket="s3://bixi-project-udacity/",
#     s3_key=["bixi_trips_2020.csv"])
# transform_trips_data = TransformDataOperator(
#     task_id='transform_trips_data',
#     dag=dag,
#     redshift_conn_id="redshift",
#     target_table="trips",
#     columns="station_start, station_end, date_start, date_end, trip_duration, is_member, weather_id",
#     raw_data="start_station_code, end_station_code, start_date, end_date, duration_sec, is_member, trunc(start_date)",
#     data_source ="public.trips_staging")
# drop_trips_staging = DropTableOperator(
#     task_id='drop_trips_staging',
#     dag=dag,
#     redshift_conn_id="redshift",
#     target_table="trips_staging")

#Stations pipeline
create_station_staging = EmptyTableOperator(
    task_id='Create_station_StagingTable_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    create_table_sql=sql_stations_staging)
# fill_staging_trips = StageToRedshiftOperator(
#     task_id='fill_staging_trips',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     target_table="public.trips_staging",
#     columns_sql="start_date, start_station_code, end_date, end_station_code, duration_sec, is_member",
#     s3_bucket="s3://bixi-project-udacity/",
#     s3_key=["bixi_trips_2020.csv"])
# transform_trips_data = TransformDataOperator(
#     task_id='transform_trips_data',
#     dag=dag,
#     redshift_conn_id="redshift",
#     target_table="trips",
#     columns="station_start, station_end, date_start, date_end, trip_duration, is_member, weather_id",
#     raw_data="start_station_code, end_station_code, start_date, end_date, duration_sec, is_member, trunc(start_date)",
#     data_source ="public.trips_staging")
# drop_trips_staging = DropTableOperator(
#     task_id='drop_trips_staging',
#     dag=dag,
#     redshift_conn_id="redshift",
#     target_table="trips_staging")

"""
Pipeline organization and tasks dependancies
"""
# Setup dependancies
# start_operator >> weather_XML_to_CSV >> upload_weather_s3 >> create_weather_empty >> setup_complete_operator
# start_operator >> upload_bixi_trips_s3 >> create_trips_empty >> setup_complete_operator
# start_operator >> upload_stations_s3 >> create_stations_empty >> setup_complete_operator

# ETL
# setup_complete_operator >> create_trips_staging >> transform_trips_data >> drop_trips_staging
setup_complete_operator >> create_station_staging