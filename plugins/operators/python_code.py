from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from scripts.download_datasets import download_covid_data

###
# To do: export PYTHONPATH=/path/to/my/scripts/dir/:$PYTHONPATH
###

class RunPythonCodeDataOperator(PythonOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 task_id='',
                 provide_context=False,
                 python_callable=download_covid_data,
                 *args, **kwargs):

        super(RunPythonCodeDataOperator, self).__init__(*args, **kwargs)
        self.task_id = task_id
        self.provide_context = provide_context
        self.python_callable = python_callable

    def execute(self, context):
        dag = DAG(self.task_id)
        task_text = '{}'.format(self.task_id)
        self.log.info(f"Executing "+task_text)
        PythonOperator(dag=dag,
               task_id=self.task_id,
               provide_context=self.provide_context,
               python_callable=self.python_callable)