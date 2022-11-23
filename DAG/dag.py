from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
#from scripts import task1

default_dag_args = {'owner': 'airflow',
                    'depends_on_past': False,
                    'start_date': airflow.utils.dates.days_ago(1),
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5)}

with DAG(
    "cloudsql_to_bigquery_dag",
    schedule_interval=None,
    default_args=default_dag_args,
) as dag:
    start = DummyOperator(
            task_id ='start',
            trigger_rule = 'all_success',
    )
    
    end = DummyOperator(
          task_id = 'end',
          trigger_rule = 'all_success',
    )
    sql_to_bq = BashOperator(
                task_id = 'sqltobq',
                bash_command="python /home/airflow/gcs/dags/tasks/mysql_to_bq_etl.py")
    
    start >> sql_to_bq >> end
