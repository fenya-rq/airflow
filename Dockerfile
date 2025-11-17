FROM apache/airflow:3.1.2-python3.12

RUN pip install --no-cache-dir psycopg[binary]==3.2.12 \
    deepdiff==8.6.1
