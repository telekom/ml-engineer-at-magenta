import logging
import sys

from dagster import get_dagster_logger


log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)

# group_name = "predict"


# @asset(
#     group_name=group_name,
#     
# )
# def predictions(classifier, df_input_preprocessed):
# 
#     return predictions_df
