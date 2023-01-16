"""
Support file that contains all the functions in order to transform the data

"""
def melt_dataframe(df, id_vars, value_vars, var_name, value_name):
    from pyspark.sql import functions as f
    # https://stackoverflow.com/questions/55378047/pyspark-dataframe-melt-columns-into-rows

    var_n_values = f.array(
        *(f.struct(f.lit(i).alias(var_name),
                   f.col(i).alias(value_name)) for i in value_vars))

    tmp_df = df.withColumn("var_n_values", f.explode(var_n_values))

    cols = id_vars + [f.col("var_n_values")[x].alias(x)
                      for x in [var_name, value_name]]

    return tmp_df.select(*cols)


def datecols(df):
    import re
    date_regex = '(?:[0-9]{1,2}/){1,2}[0-9]{2}'
    columns = df.schema.names
    date_cols = []
    for col in columns:
        match = re.match(date_regex, col)
        if match:
            date_cols.append(col)
    return date_cols


def transform_raw_data(input_path: str,
                       variable_name: str,
                       output_path: str):
    """
    Acabei dividindo de transformação para que aceita-se cada
    arquivo individualmente

    """

    from pyspark.sql import SparkSession
    from pyspark import SQLContext
    from pyspark.sql import functions as f
    from pyspark.sql.types import LongType, TimestampType

    from datetime import datetime
    spark = SparkSession.builder \
        .master('local') \
        .appName('covidpipe') \
        .config('spark.executor.memory', '28g') \
        .config('spark.driver.memory', '4g')\
        .config("spark.cores.max", "4") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    df = spark.read.options(inferSchema='true', header='true').csv(input_path)

    date_cols = datecols(df)
    var_cols = [col for col in df.columns if col not in date_cols]

    df = melt_dataframe(df,
                        id_vars=var_cols,
                        value_vars=date_cols,
                        var_name="data",
                        value_name=variable_name)

    # UDF (user defined function)
    func = f.udf(lambda x: datetime.strptime(x, '%m/%d/%y'), TimestampType())

    df = df.withColumn('data', func(f.col('data')))

    df = df.withColumn(variable_name, df[variable_name].cast(LongType()))

    # agrupamento nao necessário, não há instruções para isso
    # df = df.groupBy(var_cols).agg(f.sum(variable_name).alias(variable_name))
    df = df.withColumn('year', f.year('data'))\
        .withColumn('month', f.month('data'))
    df = df.withColumnRenamed('Country/Region', 'country')\
        .withColumnRenamed('Province/State', 'state')\
        .withColumnRenamed('Lat', 'latitude')\
        .withColumnRenamed('Long', 'longitude')

    # sem registros significativos mesmo com subset
    # df = df.dropna(subset=('pais','estado',variable_name))
    df = df.dropna()
    # df =  df.dropDuplicates(["pais","estado","data",variable_name])
    print(df.show())

    # print(f'data da execução {context.get("execution_date").strftime("%Y-%m-%d")}')
    df.coalesce(1).write.mode("overwrite").parquet(
        f'{output_path}/{variable_name}')
    spark.stop()


def create_trusted_layer(confirmed_input: str, death_input: str, recovered_input: str, output_path: str):
    """
    concat dataframes and assign partition columns
    """
    from pyspark.sql import SparkSession
    from pyspark import SQLContext
    spark = SparkSession.builder \
        .master('local') \
        .appName('muthootSample1') \
        .config('spark.executor.memory', '28g') \
        .config('spark.driver.memory', '4g')\
        .config("spark.cores.max", "4") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    confirmed = spark.read.options(
        inferSchema='true', header='true').parquet(confirmed_input)
    deaths = spark.read.options(
        inferSchema='true', header='true').parquet(death_input)
    recovered = spark.read.options(
        inferSchema='true', header='true').parquet(recovered_input)

    df_concat = confirmed.join(deaths, ['country', 'state', 'data', 'year', 'month', 'latitude', 'longitude'])\
        .join(recovered, ['country', 'state', 'data', 'year', 'month', 'latitude', 'longitude'])

    # df_concat = df_concat.withColumn("data", df_concat["data"].cast(TimestampType()))

    print(df_concat.show())
    df_concat.write.option("header", True).partitionBy(
        "year", "month").mode("overwrite").parquet(output_path)
    spark.stop()


def create_refined_layer(input_path: str, output_path: str, columns_schema: dict):
    from pyspark.sql import SparkSession
    from pyspark import SQLContext
    from pyspark.sql import functions as f
    from pyspark.sql.types import LongType
    from pyspark.sql import Window

    spark = SparkSession.builder \
        .master('local') \
        .appName('muthootSample1') \
        .config('spark.executor.memory', '28g') \
        .config('spark.driver.memory', '4g')\
        .config("spark.cores.max", "4") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    df = spark.read.options(inferSchema='true', header='true').parquet(
        f'{input_path}/*/*/*.parquet')
    df = df.withColumn('year', f.year('data'))

    # same result
    # https://stackoverflow.com/questions/58889216/rolling-average-and-sum-by-days-over-timestamp-in-pyspark
    w = Window().partitionBy(['country']).orderBy('data').rowsBetween(-6, 0)

    df = df.withColumn('moving_avg_confirmed', f.avg(columns_schema.get("confirmed")).over(w))\
        .withColumn('moving_avg_deaths', f.avg(columns_schema.get("deaths")).over(w))\
        .withColumn('moving_avg_recovered', f.avg(columns_schema.get("recovered")).over(w))
    print(df.show())

    df = df.withColumn("moving_avg_confirmed", df["moving_avg_confirmed"].cast(LongType()))\
        .withColumn("moving_avg_deaths", df["moving_avg_deaths"].cast(LongType()))\
        .withColumn("moving_avg_recovered", df["moving_avg_recovered"].cast(LongType()))

    df = df.select('country', 'data', 'moving_avg_confirmed',
                   'moving_avg_deaths', 'moving_avg_recovered', 'year')

    print(df.show())

    df.coalesce(1).write.option("header", True).partitionBy(
        "year").mode("overwrite").parquet(output_path)
    spark.stop()
