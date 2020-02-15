from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_stmt="",
                pass_value="",
                error_message="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt 
        self.pass_value = pass_value
        self.error_message = error_message

    def execute(self, context):
        self.log.info(f"Checking the data quality")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"SQL count query: {self.sql_stmt}")
        source_count = redshift.get_first(self.sql_stmt)[0]
        self.log.info(f"Source SQL count: {source_count}")

        if (source_count <= self.pass_value):
            self.log.error(f"Data quality check failed. {self.error_message}")
            raise ValueError(f"Data quality check failed. {self.error_message}")
        
        self.log.info(f"Finished checking the data quality")