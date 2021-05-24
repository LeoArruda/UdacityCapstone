from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CalculateNewCasesOperator(BaseOperator):
    ui_color = '#8700f5'
    
    update_sql = """
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 calculate_sql_stmt="",
                 *args, **kwargs):

        super(CalculateNewCasesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.calculate_sql_stmt = calculate_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = CalculateNewCasesOperator.update_sql.format(
            self.calculate_sql_stmt
        )
        
        self.log.info(f"Calculating new cases fact table and updating Redshift")
        redshift.run(formatted_sql)