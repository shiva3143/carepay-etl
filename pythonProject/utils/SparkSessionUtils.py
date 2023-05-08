"""Common module to create Spark Session"""
from pyspark.sql import SparkSession


def get_spark_session(configs: dict):
    """

    @:param configs
    :return: SparkSession

    Function to get or create the spark session object. It takes a set of dict of
     configs and create spark sessions with those configs
    """
    spark = SparkSession.builder.appName("carepay_etl")\
        .config('spark.jars','file:///C:/Users/lenovo/PycharmProjects/pythonProject/venv/Lib/site-packages/jars/spark-redshift_2.11-2.0.1.jar,file:///C:/Users/lenovo/PycharmProjects/pythonProject/venv/Lib/site-packages/jars/redshift-jdbc42-2.1.0.14.jar')
    return spark.getOrCreate()
