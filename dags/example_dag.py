import pendulum
from airflow.models.dag import DAG

with DAG(
    dag_id="example_dag_from_git",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    from airflow.operators.bash import BashOperator

    BashOperator(task_id="run_bash", bash_command="echo 'Hello from GitSync DAG!'")
