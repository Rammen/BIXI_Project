from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Create empty table in redshift
"""

class EmptyTableOperator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#b2b4b8'
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 create_table_sql="",
                 *args, **kwargs):

        super(EmptyTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        self.log.info(f"running query:{self.create_table_sql}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.create_table_sql)