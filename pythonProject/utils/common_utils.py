"""Common utility module to hold the common utilities like reusable functions, IO functions etc."""
import boto3
import yaml
import logging
from sqlalchemy import create_engine

def __get_logger():
    """ Private function to get the PySpark logger"""
    log = logging.getLogger('py4j')
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s  %(msg)s"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)
    return log


log = __get_logger()


def __read_yaml_properties(yaml_location: str) -> dict:
    print("-----------------------")
    print(yaml_location)
    """

    @:param yaml_location
    :return: dict

    Private function to read yaml properties. Takes yaml location as parameter and written dict object with all
    configs from yaml file
     """

    log.info("------ Reading configs from: {} --------".format(yaml_location))
    try:
        with open(yaml_location, "r") as f:
            config = yaml.full_load(f)
    except FileNotFoundError:
        log.error("------- Error while reading configs from {} --------".format(yaml_location))
        raise FileNotFoundError("Unable to locate Config file: {}".format(yaml_location))
    except:
        log.error("------- Error while reading configs from {} --------".format(yaml_location))
        raise Exception("Unexpected Error while reading configs from: {}".format(yaml_location))
    return config


def get_configs(config_path: str, env: str) -> dict:
    """

    @:param config_path
    @:param env
    :return: dict

    Read properties from YAML file and written env specific properties and common properties
    """

    configs = __read_yaml_properties("{}config.yaml".format(config_path))
    config_dict = configs[env].copy()
    return config_dict


def create_redshift_table(configs,df,tbname,redshift_connection_str):
    pd_df=df.toPandas()
    redshift_url = configs["redshift_url"]
    redshift_user = configs["redshift_user"]
    redshift_password = configs["redshift_password"]
    engine = create_engine(redshift_connection_str)
    pd_df.to_sql(tbname, engine, index=False, if_exists="replace")