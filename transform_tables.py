from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Transform data into fact and dimension tables
"""

class TransformDataOperator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
    transform_sql = """ 
        INSERT INTO {} ({})
        SELECT {}
        FROM {}; """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 columns="",
                 raw_data="",
                 data_source ="",
                 *args, **kwargs):

        super(TransformDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.columns = columns
        self.raw_data = raw_data
        self.data_source = data_source
              

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = TransformDataOperator.transform_sql.format(
            self.target_table,
            self.columns,
            self.raw_data,
            self.data_source)
        
        redshift.run(formatted_sql)
        

    