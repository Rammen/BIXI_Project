from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Drop table if they exist in Redshift
"""

class DropTableOperator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
#     transform_sql = """DROP TABLE IF EXISTS {}"""
    transform_sql = """TRUNCATE TABLE {}"""
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 *args, **kwargs):

        super(DropTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
              

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = DropTableOperator.transform_sql.format(self.target_table)
        redshift.run(formatted_sql)