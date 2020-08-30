from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadFactOperator(BaseOperator):
# """
# This operator load data from staging phase to fact table which is target table.

# """
    ui_color = '#F98866'
# """      Inputs:
#         * redshift_conn_id Redshift connection ID in Airflow connectors
#         * sql_stat SQL statement for loading data into target Fact table
#         * target_table name of target Fact table to load data into
#  """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_qurey="",
                 target_table="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.reshift_conn_id="redshift"
        self.sql_query=sql_query
        self.target_table=""
    def execute(self, context):
        self.log.info('Loading data into fact table:{}'.format(self.target_table))
        redshift_hook=PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.sql_query)
        self.log.info("Done inserting data into fact table: {}".format(self.target_table))

