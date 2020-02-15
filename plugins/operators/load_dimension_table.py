from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_sql="",
                 insert_sql="",
                 truncate_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_sql = insert_sql
        self.truncate_data = truncate_data

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create dimension table {self.table}")
        redshift.run(self.create_table_sql)    
        
        self.log.info(f"Loading data from staging tables to dimension {self.table}")
        if(self.truncate_data):
            self.log.info(f"Truncating data for dimension {self.table} table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        insert_query = """INSERT INTO {table} {insert_sql} """.format(
                table = self.table,
                insert_sql = self.insert_sql)

        self.log.info(f"Copy data to the dimension {self.table} table")
        redshift.run(insert_query)
        
        self.log.info(f"Finished loading data from dimension tables to the {self.table}")