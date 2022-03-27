from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
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
        time_format="",
        region="us-east-1",
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.iam_role = iam_role
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.time_format = time_format
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)

        copy_sql = f"""
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{rendered_key}'
            CREDENTIALS 'aws_iam_role={self.iam_role}'
            FORMAT AS CSV
            REGION '{self.region}'
        """

        self.log.info("Loading data from S3 to Redshift table")
        redshift.run(copy_sql)
