from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone


with DAG(
    # require parameters
    dag_id="my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2023, 5, 1),  # use utils date time from airflow
    tags=["DEB", "Skooldio"]
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2