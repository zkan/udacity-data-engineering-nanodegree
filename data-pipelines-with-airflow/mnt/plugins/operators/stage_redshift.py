from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    ui_color = "#358140"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        iam_role="",
        s3_bucket="",
        s3_key="",
        json_path="",
        time_format="",
        region="us-west-2",
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.iam_role = iam_role
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.time_format = time_format
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)

        if self.json_path:
            copy_sql = f"""
                COPY {self.table}
                FROM 's3://{self.s3_bucket}/{rendered_key}'
                CREDENTIALS 'aws_iam_role={self.iam_role}'
                FORMAT AS JSON 's3://{self.s3_bucket}/{self.json_path}'
                TIMEFORMAT '{self.time_format}'
                REGION '{self.region}'
            """
        else:
            copy_sql = f"""
                COPY {self.table}
                FROM 's3://{self.s3_bucket}/{rendered_key}'
                CREDENTIALS 'aws_iam_role={self.iam_role}'
                FORMAT AS JSON 'auto'
                REGION '{self.region}'
            """

        self.log.info("Loading data from S3 to Redshift table")
        redshift.run(copy_sql)
