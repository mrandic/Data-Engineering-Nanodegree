from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This class implements operator for data quality checks.
    It inherits BaseOperator class.
    :param redshift_conn_id: Connection ID for Redhift
    :param test_sql_array: array of SQLs to be tested against referent values
    :param expected_output: array of expected, referent values used for data quality check.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id  = "",
                 test_sql_array    = [],
                 expected_output   = [],
                 *args, 
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.test_sql_array     = test_sql_array
        self.expected_output    = expected_output

    def execute(self, context):
        """
        This function checks for data quality.
        Check considers whether there is no records or there is a mismatch in data.
        """
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        for index, test_sql in enumerate(self.test_sql_array):
            
            self.log.info(f"Running data quality check {index}: {test_sql}")
            records = redshift.get_records(test_sql)
            
            if len(records) < 1 or len(records[0]) < 1:
               raise ValueError(f"Error: {test_sql} returned no results.")
            
            if not self.expected_output[index](records[0][0]):
               raise ValueError(f"Error: {test_sql} expected value mismatched returned {records[0][0]}")
            
            self.log.info(f"Data quality check for query {test_sql} finished successfully.")