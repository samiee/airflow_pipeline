from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
"""
    This operator is able to load given dataset in JSON or CSV format 
    from specified location on S3 into target Redshift table    
"""
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 source_file='',
                 target_table='stageing_table',
                 file_type=('json','csv'),
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # instantiation class variables
        self.redshift_conn_id=reshift_conn_id,
        self.aws_conn_id=aws_conn_id,
        self.source_file=source_file,
        self.target_table=target_table,
        self.file_type=file_type,
        self.json_path=json_path
    def execute(self, context):
        if file_type in ["json","csv"]:
            redshift_hook=PostgressHook(self.redshift_conn_id)
            aws_hook=AwsHook(self.aws_conn_id)
            credentials=aws_hook.get_credential()
            self.log.info(f'StageToRedshiftOperator loading tables {self.source_file} to staging table: {self.target_table}')
            redshift_hook.run(SqlQueries.trancuate_table.format(self.target_table))
            if self.file_type=='json':
                if self.json_path !='':
                    redshift.run(SqlQueries.copy_from_json_with_json_path_to_redshift.format(
                        self.target_table
                        ,self.source_file
                        ,credentials.access_key
                        ,credentials.secret_key
                        ,self.json_path))
                else(
                    redshift.run(SqlQueries.copy_from_json_to_redshift.format(
                        self.target_table
                        ,self.source_file
                        ,credentials.ACCESS_KEY
                        ,credentials.SECRET_KEY
                        ))
            elif self.file_type='csv':
                redshift.run(SqlQueries.csv.format(
                        self.target_table,
                        self.source_file,
                        credentials.ACCESS_KEY,
                        credentials.SECRET_KEY
                        
                )                             
             self.log.info(f'StageToRedshiftOperator has completed at: {self.source_file} to staging table: {self.target_table}')
        else:
            raise ValueError("file_type param must be either json or csv")
