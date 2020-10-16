import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        *args,
        table="",
        redshift_conn_id="redshift",
        select=None,
        truncate=True,
        **kwargs,
    ):
        """Operator for loading dimension tables with staged data.

        Args:
            table (str, optional): Name of dimension table. Defaults to "".
            redshift_conn_id (str, optional): Name of saved Redshift connection. Defaults to "redshift".
            select (str, optional): The select statement to be used in INSERT INTO SELECT. Defaults to None.
            truncate (bool, optional): Truncate dimension table before loading. Defaults to True.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.select = select
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        if self.truncate:
            logging.info(f"Truncating {self.table}")
            redshift_hook.run(f"truncate table {self.table}")

        logging.info(f"Inserting data into {self.table}...")
        redshift_hook.run(
            f"""
                insert into {self.table}
                {self.select};
            """
        )
        logging.info(f"Inserted data into {self.table}")
