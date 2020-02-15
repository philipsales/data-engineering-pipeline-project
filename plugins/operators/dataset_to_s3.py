from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DatasetToS3Operator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 input_data_path="",
                 s3_folder="",
                 s3_source="",
                 file_type= "",
                 *args, **kwargs):
        super(DatasetToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.input_data_path = input_data_path
        self.s3_source = s3_source
        self.s3_folder = s3_folder
        self.file_type = file_type

    def execute(self, context):
        """
        Load the data files onto the chosen storage location.
        """

        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        s3_hook.load_file(
            #filename='./datasets/processed_data/airport/part-00000-d49b97ad-3f4a-4626-a106-2d6c797bb891-c000.json',
            #filename='./datasets/processed_data/immigration/part-00000-1d147861-0f2a-477c-a146-6a23ecd8d329-c000.json',
            filename='./datasets/processed_data/temperature/part-00000-5a397edf-8372-449f-af12-8560562022e9-c000.json',
            #key="airport/part-00000-d49b97ad-3f4a-4626-a106-2d6c797bb891-c000.json",
            #key="immigration/part-00000-1d147861-0f2a-477c-a146-6a23ecd8d329-c000.json",
            key="temperature/part-00000-5a397edf-8372-449f-af12-8560562022e9-c000.json",
            bucket_name='udend-datasets',
            replace=True,
            encrypt=False
        )
        self.log.info(f"Finished loading data from S3 to the")