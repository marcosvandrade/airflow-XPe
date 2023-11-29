from datetime import datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 28),
    # Add other default arguments as needed
}

dag = DAG('tpm1_workflow', default_args=default_args)

capture_task = PapermillOperator(
    task_id='run_capture',
    input_nb='/home/marcos/airflow/notebooks/capture.ipynb',
    output_nb = '/home/marcos/airflow/notebooks/capture_output.ipynb',
    # parameters={'param1': 'value1', 'param2': 'value2'},  # Optional parameters
    dag=dag
)

silver_task = PapermillOperator(
    task_id='run_silver',
    input_nb='/home/marcos/airflow/notebooks/silver.ipynb',
    output_nb = '/home/marcos/airflow/notebooks/silver_output.ipynb',
    # parameters={'param1': 'value1', 'param2': 'value2'},  # Optional parameters
    dag=dag
)

gold_task = PapermillOperator(
    task_id='run_gold',
    input_nb='/home/marcos/airflow/notebooks/gold.ipynb',
    output_nb = '/home/marcos/airflow/notebooks/gold_output.ipynb',
    # parameters={'param1': 'value1', 'param2': 'value2'},  # Optional parameters
    dag=dag
)

capture_task >> silver_task >> gold_task