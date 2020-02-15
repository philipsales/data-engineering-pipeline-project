from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_sql="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.create_table_sql = create_table_sql 

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create fact table {self.table}")
        redshift.run(self.create_table_sql)    

        self.log.info(f"Loading data from staging tables to the {self.table}")
        insert_query = """INSERT INTO {table} {insert_sql} """.format(
                table = self.table,
                insert_sql = self.insert_sql)
        self.log.info(f"Final insert query: {insert_query}")

        self.log.info(f"Copy data to the fact table: {self.table}")
        redshift.run(insert_query)
        
        self.log.info(f"Finished loading data from staging tables to the {self.table}")