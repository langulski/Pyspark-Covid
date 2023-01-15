FROM apache/airflow:2.5.0-python3.10

RUN pip install -- no-cache-dir pyspark==3.3.1

USER root


RUN apt-get update && \
    apt-get install default-jdk -y &&\
    apt-get update && \
    apt-get install -y ant && \
    apt-get clean;
    
# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;