## Pyspark COVI19 Airflow pipeline

## Project status:
:heavy_check_mark: complete

## Table of Contents
- [Objective](#Objective)
- [Quick-Start](#Quick-Start)
- [Process](#Process)
- [Learning Process](#Learning-Process)
- [Author](#Author)

## Objective

Create and transform data from covid19 from raw layers to refined and trusted layers.

## Quick-Start

Make sure you have docker installed in your OS

```bash
git clone https://github.com/langulski/Pyspark-Covid.git
```

Before composing docker, set your .env variables:
```bash
AIRFLOW_UID=1001
POSTGRES_USER='airflow'
POSTGRES_PASSWORD='airflow'
POSTGRES_DB='airflow'
_AIRFLOW_WWW_USER_USERNAME='airflow'
_AIRFLOW_WWW_USER_PASSWORD='airflow'
_AIRFLOW_WWW_USER_EMAIL=email

```
Now you are able to deploy Airflow executing the command line
```bash
docker-compose up
```

## Process

- Extract data from raw;
- Create pyspark script to transform data to refined and trusted layers;
- Create Airflow tasks;
- Check results;


#### 1.2 Dag Airflow

How the pipeline works:

1. Checking if file exists inside raw folder
2. task group (extract and transform to processed layer)
3. transform data to 'trusted'
4. transform data to 'refined'



## Learning Process
 
Transforming data columns to rows was somewhat challenging, and since pyspark does not have a native
'melt dataframe' function it was necessary go through stackoverflow for solutions.


## Author
Lucas Angulski 