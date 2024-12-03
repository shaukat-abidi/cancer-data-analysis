import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Default arguments for the DAG
default_args = {
    "owner": "test",
    "depends_on_past": True,
}

# Define the DAG
with DAG(
    dag_id="Test_ETL",
    description="Testing ETL in Airflow",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
) as dag:

    # Define task functions
    def extract_data(**kwargs):
        # READ CSV HERE
        time.sleep(5)

    def process_data(**kwargs):
        # Apply all transformations here
        time.sleep(5)

    def load_data(**kwargs):
        # Load the data into datafile or datastore (CSV/SQL etc)
        time.sleep(5)

    # Define a reusable function to create ETL tasks
    def create_etl_tasks(entity_name):
        extract_task = PythonOperator(
            task_id=f"extract_{entity_name}_info",
            python_callable=extract_data,
            provide_context=True,
        )

        process_task = PythonOperator(
            task_id=f"process_{entity_name}_info",
            python_callable=process_data,
            provide_context=True,
        )

        load_task = PythonOperator(
            task_id=f"load_{entity_name}_info",
            python_callable=load_data,
            provide_context=True,
        )

        # Set task dependencies
        extract_task >> process_task >> load_task

        return extract_task, process_task, load_task

    # Create ETL tasks for each entity
    entities = ["encounters", "medications", "organisations", "patients", "procedures"]

    for entity in entities:
        create_etl_tasks(entity)
