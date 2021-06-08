from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.decorators import apply_defaults
from scripts.download_datasets import download_covid_data

###
# To do: export PYTHONPATH=/path/to/my/scripts/dir/:$PYTHONPATH
###

class RunSparkCodeDataOperator(SparkSubmitOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 task_id='',
                 application='',
                 deploy_mode='cluster',
                 executor_cores= 1,
                 executor_memory= '2G',
                 master='yarn',
                 *args, **kwargs):

        super(RunSparkCodeDataOperator, self).__init__(*args, **kwargs)
        self.task_id = task_id
        self.application = application
        self.deploy_mode = deploy_mode
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.master = master

    def execute(self, context):
        dag = DAG(self.task_id)
        task_text = '{}'.format(self.task_id)
        self.log.info(f"SPARK: Executing "+task_text)

        _config ={'application':self.application,
            'master' : self.master,
            'deploy-mode' : self.deploy_mode,
            'executor_cores': self.executor_cores,
            'EXECUTORS_MEM': self.executor_memory
        }

        SparkSubmitOperator(
               task_id=self.task_id,
               dag=dag,
               **_config)
