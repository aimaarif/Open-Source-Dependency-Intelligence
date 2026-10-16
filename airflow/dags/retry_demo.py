# Airflow DAG definition for testing retry behavior.
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="airflow_retry_demo",
    description="Test Airflow retry behavior without external data access.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["testing"],
)
def airflow_retry_demo():

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def simulate_transient_failure():
        context = get_current_context()
        attempt = context["ti"].try_number

        print(f"Running attempt {attempt}")

        if attempt == 1:
            raise RuntimeError(
                "Intentional test failure on the first attempt."
            )

        print("Retry succeeded. No external data was modified.")

    simulate_transient_failure()


airflow_retry_demo()
