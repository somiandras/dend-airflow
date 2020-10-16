import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        *args,
        redshift_conn_id="redshift",
        queries=None,
        expected=None,
        table=None,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries
        self.expected = expected

    def execute(self, context):
        postgres_hook = PostgresHook(self.redshift_conn_id)
        for query, expected in zip(self.queries, self.expected):
            results = postgres_hook.get_records(query)
            if results is None:
                raise AirflowFailException(
                    f"Quality check did not return any results: {query}"
                )
            elif results[0][0] != expected:
                raise AirflowFailException(
                    f"Quality check failed: {results[0][0]} != {expected} (expected), query: {query}"
                )
            else:
                logging.info("Quality check passed")
