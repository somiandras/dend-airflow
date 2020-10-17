import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        *args,
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="",
        s3_bucket="",
        s3_key="",
        jsonpath="auto",
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.jsonpath = jsonpath

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        creds = aws_hook.get_credentials()
        rendered_key = self.s3_key.format(**context)
        source_path = f"s3://{self.s3_bucket}/{rendered_key}/"
        logging.info(f"Copying data from {source_path}")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(
            f"""truncate{self.table};
                copy {self.table}
                from '{source_path}'
                access_key_id '{creds.access_key}'
                secret_access_key '{creds.secret_key}'
                json '{self.jsonpath}';
            """
        )
        logging.info(f"Copied data from {source_path}")
