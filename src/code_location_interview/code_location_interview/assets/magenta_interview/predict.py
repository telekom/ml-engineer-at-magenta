import logging
import sys
from datetime import datetime
import pandas as pd

from dagster import get_dagster_logger, asset, AutomationCondition


log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)

group_name = "predict"


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def predictions(features, deployed_model):
    features = features.set_index("rating_account_id")
    y_pred = deployed_model.predict_proba(features)[:, 1]

    predictions_df = pd.DataFrame()
    predictions_df["rating_account_id"] = features.index
    predictions_df["churn_risk"] = y_pred
    predictions_df["run_dt"] = datetime.now()

    return predictions_df
