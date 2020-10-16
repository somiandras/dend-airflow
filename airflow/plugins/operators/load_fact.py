from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self, *args, table="", redshift_conn_id="redshift", select=None, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.select = select
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        redshift_hook.run(
            f"""
                insert into {self.table}
                {self.select};
            """
        )
