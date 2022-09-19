from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

cassandra_host = "cassandra"
cassandra_user = "cassandra"
cassandra_pwd  = "cassandra"
cassandra_port = 9042
key_space      = "logs_analytics"
table_name     = "nasa_logs"
kafka_server   = "kafka:9092"
kafka_topic    = "logs_analytics"
postgres_user  = 'postgres'
postgres_pass = 'postgres'
postgres_db    = 'logs_analytics'
postgres_table = 'nasa_logs'
postgres_url   = f'jdbc:postgresql://postgres:5432/{postgres_db}'


def process_row(df, epoch_id):
    """
    Writes data to Cassandra and HDFS location
    Parameters
    ----------
    df : DataFrame
        Streaming Dataframe
    epoch_id : int
        Unique id for each micro batch/epoch
    """
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="nasalog", keyspace="loganalysis") \
        .save()  # hot path
    df.write.csv("hdfs://namenode:8020/output/nasa_logs/", mode="append")  # cold path


    df.write.format("jdbc") \
        .option("url", postgres_url) \
        .option("driver", 'org.postgresql.Driver') \
        .option("dbtable", postgres_table) \
        .mode('append') \
        .option("user", postgres_user) \
        .option("password", postgres_pass) \
        .save()

def get_spark_object(app_name):
    """
    Return spark object
    Parameters
    ----------
    app_name : string
        App name of applicaion
    """
    # Spark Session creation configured to interact with
    spark = SparkSession.builder.appName(app_name). \
        config("spark.jars.packages",
               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector-driver_2.12:3.0.0"). \
        config("spark.cassandra.connection.host", cassandra_host). \
        config("spark.cassandra.auth.username", cassandra_user). \
        config("spark.jars", "/opt/workspace/lib/postgresql-42.3.5.jar"). \
        config("spark.cassandra.auth.password", cassandra_pwd). \
        getOrCreate()

    return spark

def read_kafka(spark):
    """
    Return kafka stream
    Parameters
    ----------
    spark : spark object
    """
    # Read data from Kafka topic
    split_logic = split(col("url"), "\.").getItem(1)
    log_data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("split(value,',')[1] as host",
                    "split(value,',')[2] as time",
                    "split(value,',')[3] as method",
                    "split(value,',')[4] as url",
                    "split(value,',')[5] as response",
                    "split(value,',')[6] as bytes"
                    ) \
        .withColumn("time_added", unix_timestamp()) \
        .withColumn("extension", when(split_logic.isNull(), "None").otherwise(split_logic))

    return log_data

def main():
    app_name = 'logs-analytics'
    spark = get_spark_object(app_name)

    log_data = read_kafka(spark)

    # Writes streaming dataframe to ForeachBatch console which ingests data to Cassandra
    log_data \
        .writeStream \
        .option("checkpointLocation", "checkpoint/data") \
        .foreachBatch(process_row) \
        .start() \
        .awaitTermination()

if __name__ == "__main__" :
    logging.info("logs_analytics is Started ...")
    main()

