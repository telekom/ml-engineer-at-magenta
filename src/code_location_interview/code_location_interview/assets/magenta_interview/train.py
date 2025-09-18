import logging
import sys

from dagster import get_dagster_logger

# from dagstermill import define_dagstermill_asset

log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)

# group_name = "training"


# @multi_asset(
#     group_name=group_name,
#     outs={
#         "train_data": AssetOut(
#             
#         ),
#         "test_data": AssetOut(
#             
#         ),
#     },
# )
# def split_train_test(df_input_preprocessed):
#
#     return train_data, test_data
# 
# 
# @asset(group_name=group_name)
# def classifier(train_data):
# 
#     return pipeline
