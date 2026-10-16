FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    boto3 \
    python-dotenv \
    requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
