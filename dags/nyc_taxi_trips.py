from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc_taxi_trips"],
)
def nyc_taxi_trips():

    @task
    def get_nyc_trip_sources():
        import subprocess
        subprocess.run(["python", "include/scripts/getNYCTripSources.py"], check=True)
    get_nyc_trip_sources()


# Appeler le dag
nyc_taxi_trips()