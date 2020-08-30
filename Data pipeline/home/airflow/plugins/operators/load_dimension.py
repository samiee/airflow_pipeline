from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadDimensionOperator(BaseOperator):
"""
This operator meant by loading dimension table from a fact table. 
this could be done by running SQL queries to the target table.
and also tuncate the dim tables befor inserting or loading data.
"""
    ui_color = '#80BD9E'
"""
        Inputs:
        * redshift_conn_id Redshift connection ID in Airflow connectors
        * sql_query SQL statement for loading data into target Fact table
        * target_table name of target Fact table to load data into
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_query="",
                 target_table=""
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tuncuate=truncate
        self.sql_query=sql_query
        self.target_table=target_table
        
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator start loading data')  redshift_hook=PostgresHook(self.redshift_conn_id)
        redshift_hook.run(SqlQueries.tuncate_table(self.target_table)) 
 redshift_hook.run(SqlQueries.sql_query(self.target_table)) 
        self.log.info(" inserting data into dimension table: {}".format(self.target_table))