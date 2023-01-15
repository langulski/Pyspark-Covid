FROM apache/airflow:2.5.0-python3.10

RUN pip install -- no-cache-dir pyspark==3.3.1


