from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
This Operator is  meant by checking dim tables that we moved from the fact tables.
It takes two pair of arguments as sql queries and  it runs on Redshift then compared with the expected value
where there's mismatching error message will raise
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
      """
        Inputs:
        * redshift_conn_id Redshift connection ID of Airflow connector for Redshift
        * sql_queries list of pairs of SQL statement and expected value to be test for equality after query execution
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        Map params here
        Example:
        self.redshift_conn_id =redshift_conn_id
        self.sql_queries=sql_queries
        
        
    def execute(self, context):
        redshift_hook=PostgresHook(self.redshift_conn_id)
        for (sql_query,result) in self.sql_queries:
            row=redshift_hook.get_first(sql_query)
            if row is not None and row[0] == result:
                self.log.info('The Result {} is matched with expected Result {}\n==================================='.format(sql_query,result)
            else:
                 raiseValueErorr('Test Faild : {} not equal {} \n==================================='.format(sql_query,result))               