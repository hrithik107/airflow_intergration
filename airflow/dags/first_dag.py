try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))



from scheduler import scheduler

def scheduler_running():
    scheduler()
    return "it works"


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={

            "owner":"airflow",
            "retries":1,
            "retry_delay":timedelta(minutes=2),
            "start_date": datetime(2021, 1,1),
        },
        catchup=False) as f:

    scheduler_running=PythonOperator(
        task_id="scheduler_running",
        python_callable=scheduler_running,)

