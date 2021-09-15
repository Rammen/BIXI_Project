from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Load data from S3 in Redshift
"""

class LoadDataOperator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_query="",
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        self.log.info(f"running query:{self.sql_query}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql_query)