FROM apache/airflow:2.9.1-python3.11
USER root
RUN apt-get update && apt-get install -y git && apt-get clean
USER airflow
RUN pip install --no-cache-dir \
    dbt-postgres \
    pandas \
    requests \
    sqlalchemy \
    psycopg2-binary