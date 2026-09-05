FROM apache/airflow:3.3.1-python3.11

USER airflow
COPY infra/airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
