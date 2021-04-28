from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from my_script import my_python_function

###
# To do: export PYTHONPATH=/path/to/my/scripts/dir/:$PYTHONPATH
###

class DownloadAllJHUCovidDataOperator(PythonOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 task_id='Download_All_JHU_Covid_Data',
                 provide_context=False,
                 python_callable=my_python_function,
                 *args, **kwargs):

        super(DownloadAllJHUCovidDataOperator, self).__init__(*args, **kwargs)
        self.task_id = task_id
        self.provide_context = provide_context
        self.python_callable = python_callable

    def execute(self, context):
        dag = DAG('DownloadAllJHUCovidData')
        self.log.info(f"Loading dimension table {self.table}")
        PythonOperator(dag=dag,
               task_id=self.task_id,
               provide_context=self.provide_context,
               python_callable=self.python_callable)