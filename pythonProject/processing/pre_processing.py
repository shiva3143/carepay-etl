from utils.common_utils import get_configs,create_redshift_table
from utils.SparkSessionUtils import get_spark_session
from pyspark.sql import functions as f
import pandas as pd
from sqlalchemy import create_engine


def main(config_path: str, env: str):
    """
    :param config_path:
    :param env:
    :return:
     Main Function to trigger the Pre Processing. I/O calls & configs calls are part of this function.
    """

    configs = get_configs(config_path, env)
    mysql_host = configs["mysql_host"]
    mysql_port = configs["mysql_port"]
    mysql_database = configs["mysql_database"]
    mysql_username = configs["mysql_username"]
    mysql_password = configs["mysql_password"]
    redshift_url = configs["redshift_url"]
    redshift_user = configs["redshift_user"]
    redshift_password = configs["redshift_password"]
    redshift_temp_dir = configs["redshift_temp_dir"]
    spark = get_spark_session(configs)
    redshift_connection_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_url}"
    invoice_df, invoice_item_df, claim_df, treatment_df = read_input_data(spark, mysql_host, mysql_port, mysql_database, mysql_username, mysql_password)
    #create_redshift_table(configs,invoice_df, "INVOICE", redshift_connection_str)
    # create_redshift_table(configs,invoice_item_df, "raw_layer.INVOICE_ITEM", redshift_connection_str)
    # create_redshift_table(configs,claim_df, "raw_layer.CLAIM", redshift_connection_str)
    # create_redshift_table(configs,treatment_df, "raw_layer.TREATMENT", redshift_connection_str)
    # invoice_df, invoice_item_df, claim_df, treatment_df = pre_process_data(invoice_df,invoice_item_df,claim_df,treatment_df)
    # create_redshift_table(configs,invoice_df, "staging.INVOICE", redshift_connection_str)
    # create_redshift_table(configs,invoice_item_df, "staging.INVOICE_ITEM", redshift_connection_str)
    # create_redshift_table(configs,claim_df, "staging.CLAIM", redshift_connection_str)
    # create_redshift_table(configs,treatment_df, "staging.TREATMENT", redshift_connection_str)


def read_input_data(spark, hostname, port_no, dbname, username, password):
    invoice_df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{hostname}:{port_no}/{dbname}",
        driver="com.mysql.jdbc.Driver",
        dbtable="INVOICE",
        user=username,
        password=password
    ).option("zeroDateTimeBehavior", "convertToNull").load()

    invoice_item_df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{hostname}:{port_no}/{dbname}",
        driver="com.mysql.jdbc.Driver",
        dbtable="INVOICE_ITEM",
        user=username,
        password=password
    ).option("zeroDateTimeBehavior", "convertToNull").load()

    claim_df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{hostname}:{port_no}/{dbname}",
        driver="com.mysql.jdbc.Driver",
        dbtable="CLAIM",
        user=username,
        password=password
    ).option("zeroDateTimeBehavior", "convertToNull").load()

    treatment_df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{hostname}:{port_no}/{dbname}",
        driver="com.mysql.jdbc.Driver",
        dbtable="TREATMENT",
        user=username,
        password=password
    ).option("zeroDateTimeBehavior", "convertToNull").load()
    return invoice_df, invoice_item_df, claim_df, treatment_df


def pre_process_data(invoice_df,invoice_item_df,claim_df,treatment_df):
    invoice_item_df = invoice_item_df.filter(
        invoice_item_df.ID.isNotNull() & invoice_item_df.INVOICE_ID.isNotNull() & invoice_item_df.PRODUCT_ID.isNotNull() & invoice_item_df.AMOUNT.isNotNull() & invoice_item_df.DATE_CREATED.isNotNull())
    invoice_df = invoice_df.filter(invoice_df.ID.isNotNull() & invoice_df.DATE_CREATED.isNotNull())
    claim_df = claim_df.filter(
        claim_df.ID.isNotNull() & claim_df.STATUS.isNotNull() & claim_df.TREATMENT_ID.isNotNull() & claim_df.AMOUNT.isNotNull() & claim_df.DATE_CREATED.isNotNull())
    treatment_df = treatment_df.filter(
        treatment_df.ID.isNotNull() & treatment_df.STATUS.isNotNull() & treatment_df.TYPE.isNotNull() & treatment_df.DATE_CREATED.isNotNull())
    claim_df = claim_df.drop("DATE_CREATED")
    claim_df = claim_df.join(
        treatment_df.select("ID", "DATE_CREATED").withColumnRenamed("ID", "tID"),
        f.col("TREATMENT_ID") == f.col("tID"),
        "left"
    )
    claim_df = claim_df.drop("tID")
    claim_df = claim_df.filter(f.col("AMOUNT") > 0)
    invoice_df = invoice_df.drop("DATE_CREATED")
    invoice_df = invoice_df.join(
        treatment_df.select("ID", "DATE_CREATED").withColumnRenamed("ID", "tID"),
        f.col("TREATMENT_ID") == f.col("tID"),
        "left"
    )
    invoice_df = invoice_df.drop("tID")
    invoice_item_df = invoice_item_df.drop("DATE_CREATED")
    invoice_item_df = invoice_item_df.join(
        invoice_df.select("ID", "DATE_CREATED").withColumnRenamed("ID", "ivID"),
        f.col("INVOICE_ID") == f.col("ivID"),
        "left"
    )
    invoice_item_df = invoice_item_df.drop("ivID", "STATUS")
    invoice_item_df = invoice_item_df.filter((f.col("PRODUCT_ID") > 0) & (f.col("AMOUNT") > 0))
    treatment_df = treatment_df.withColumn("DATE_TREATMENT", f.col("DATE_CREATED"))
    treatment_df = treatment_df.drop("PROGRAM_ID", "TYPE")
    treatment_df = treatment_df.join(
        claim_df.select("TREATMENT_ID", "PROGRAM_ID", "TYPE").withColumnRenamed("TREATMENT_ID", "ctID"),
        f.col("ID") == f.col("ctID"),
        "left"
    )
    treatment_df = treatment_df.drop("ctID")
    return invoice_df,invoice_item_df,claim_df,treatment_df
