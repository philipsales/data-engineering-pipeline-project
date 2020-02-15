import os
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
                 s3_bucket="",
                 file_type= "",
                 *args, **kwargs):
        super(DatasetToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.file_type = file_type
        self.directory = input_data_path

    def execute(self, context):
        self.log.info("Load dataset to s3")
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        for filename in os.listdir(self.directory + '/' + self.s3_folder):
            if filename.endswith(".json"):

                self.log.info(f"JSON directory: " + os.path.join(self.directory, self.s3_folder, filename))

                s3_hook.load_file(
                    filename=self.directory + "/{}/{}".format(self.s3_folder, filename),
                    key="{}/{}".format(self.s3_folder, filename),
                    bucket_name=self.s3_bucket,
                    replace=True,
                    encrypt=False
                )
                self.log.info(f"Finished loading data from S3 to the")
            else:
                continue
