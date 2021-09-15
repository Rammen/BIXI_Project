from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Load Dimension Operator:
Take data from S3 and copy the data into new table in Redshift 
Requires the empty table in redshift to be created prior
"""

class StageToRedshiftOperator(BaseOperator):

    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
    # SQL statement to copy data
    copy_sql = """
        COPY {} ({})
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        FORMAT AS csv
        DELIMITER ','
        IGNOREHEADER 1
        timeformat 'YYYY-MM-DD HH:MI:SS';  """

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 target_table="",
                 columns_sql="",
                 s3_bucket="",
                 s3_key=[],
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.columns_sql = columns_sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        for file in self.s3_key:
            # Copy data from S3 to redshift
            self.log.info(f"Copying data from S3 (file: {file}) to Redshift (table destination: {self.target_table})")
            
            s3_path = self.s3_bucket + file

            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.target_table,
                self.columns_sql,
                s3_path,
                aws_credentials.access_key,
                aws_credentials.secret_key)
            
            self.log.info(formatted_sql)
        
            # Run query
            redshift.run(formatted_sql)