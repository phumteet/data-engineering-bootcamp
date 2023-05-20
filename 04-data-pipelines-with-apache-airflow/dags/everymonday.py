from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _world () : 
    print('world')



with DAG(
    # require parameters
    dag_id="everymonday",
    schedule="0 0 * * 1",
    start_date=timezone.datetime(2023, 5, 1),  # use utils date time from airflow
    tags=["DEB", "Skooldio"],
    catchup=False,  # default = True => รันย้อนหลังให้
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo hello",
    )
    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello >> world
