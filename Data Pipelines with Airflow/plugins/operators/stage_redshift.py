from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook    import PostgresHook
from airflow.models                 import BaseOperator
from airflow.utils.decorators       import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    This class implements operator for staging data from S3 to Redshift.
    It inherits BaseOperator class.
    
    :param redshift_conn_id: Connection ID for Redhift
    :param aws_credentials_id: AWS credentals
    :param region: AWS region
    :param s3_bucket: S3 bucket name
    :param s3_key: S3 key containing specific subdirectory
    :param table: destination table where data is to be copied
    :param data_format: format of data
    :param truncate: table truncation flag
    
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id    = "",
                aws_credentials_id  = "",
                region              = "",
                s3_bucket           = "",
                s3_key              = "",
                table               = "",
                data_format         = "auto",
                truncate            = False,
                *args, 
                **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region             = region
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
        self.table              = table
        self.data_format        = data_format
        self.truncate           = truncate


    def execute(self, context):
        """
        This function ecexutes COPY ommand from S3 to Redshift.
        
        """
        
        redshift    = PostgresHook              (postgres_conn_id = self.redshift_conn_id)
        aws_hook    = AwsHook                   (self.aws_credentials_id)
        credentials = aws_hook.get_credentials  ()

        if self.truncate:
            self.log.info (f'Truncate table {self.table}')
            redshift.run  (f'TRUNCATE {self.table}')
            
        # Format SQL query
        sql_formated    = """
                            COPY {}
                            FROM '{}'
                            ACCESS_KEY_ID '{}'
                            SECRET_ACCESS_KEY '{}'
                            REGION '{}'
                            FORMAT AS JSON '{}'
                          """.format(
                                    self.table, 
                                    f's3://{self.s3_bucket}/{self.s3_key.format(**context)}', 
                                    credentials.access_key,
                                    credentials.secret_key, 
                                    self.region,
                                    self.data_format
                                  )

        # Run COPY command and transfer data from S3 to Redshift
        self.log.info('Process copy procedure to Redshift table {self.table}')
        redshift.run(sql_formated)
