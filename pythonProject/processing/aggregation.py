from utils.SparkSessionUtils import get_spark_session
from utils.common_utils import get_configs, create_redshift_table
from processing.pre_processing import read_input_data,pre_process_data
import pandas as pd
import pyspark.sql.functions as f


def main(config_path: str, env):
    """

    :param config_path:
    :param env:
    :return:
    Main Function to trigger the Events Analyser. I/O calls & configs calls are part of this function.
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
    spark = get_spark_session(configs)
    invoice_df, invoice_item_df, claim_df, treatment_df = read_input_data(spark, mysql_host, mysql_port, mysql_database, mysql_username, mysql_password)
    invoice_df, invoice_item_df, claim_df, treatment_df = pre_process_data(invoice_df,invoice_item_df,claim_df,treatment_df)
    invoice_agg_df, claim_agg_df = aggregate_data(invoice_df,invoice_item_df,claim_df,treatment_df)
    print(f"invoice {invoice_agg_df.count()}")
    print(f"claim_agg_df {claim_agg_df.count()}")
    redshift_connection_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_url}"
    create_redshift_table(configs,invoice_agg_df, "invoice_agg", redshift_connection_str)
    create_redshift_table(configs,claim_agg_df, "claim_agg", redshift_connection_str)


def aggregate_data(invoice_df,invoice_item_df,claim_df,treatment_df):
    invoice_agg_df = invoice_df.groupBy("STATUS") \
        .agg(f.count("*").alias("count"), \
             f.max(invoice_df.DATE_CREATED).alias("latest_date_created"))
    # Perform aggregations on CLAIM dataframe
    claim_agg_df = claim_df.groupBy("STATUS") \
        .agg(f.count(claim_df.ID).alias("total_claims"), f.sum(claim_df.AMOUNT).alias("total_amount"),
             f.avg(claim_df.AMOUNT).alias("avg_amount"), \
             f.max(claim_df.AMOUNT).alias("max_amount"), f.min(claim_df.AMOUNT).alias("min_amount"))
    return invoice_agg_df, claim_agg_df


