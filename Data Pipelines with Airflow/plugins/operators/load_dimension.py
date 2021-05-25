from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This class implements operator for loading dimensional tables into Redshift.
    It inherits BaseOperator class.
    :param redshift_conn_id: Connection ID for Redhift
    :param sql: SQL query for data load
    :param table: destination table for data load
    :param truncate: table truncation flag
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = "",
                 sql                = "",
                 table              = "",
                 truncate           = True,
                 *args, 
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.sql                = sql
        self.table              = table
        self.truncate           = truncate

    def execute(self, context):
        """
        This function performs execution of data load.
        It checks wherther table truncation is needed and then insertion is performed on target table.
        """
        postgres = PostgresHook (postgres_conn_id = self.redshift_conn_id)

        if self.truncate:
            self.log.info (f'Truncate dim table {self.table}')
            postgres.run  (f'TRUNCATE {self.table}')

        self.log.info (f'Load dim table {self.table}')
        postgres.run  (f'INSERT INTO {self.table} {self.sql}')
