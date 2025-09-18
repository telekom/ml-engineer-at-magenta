import logging
import sys

from dagster import get_dagster_logger, AutomationCondition, AssetOut, Output, asset, multi_asset
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score


log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)

group_name = "training"


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.on_cron("0 1 8-14,22-28 * 1")
)
def df_input(features, label):
    inputs = features.merge(label, on="rating_account_id")
    return inputs


@multi_asset(
    group_name=group_name,
    outs={
        "train_data": AssetOut(
            automation_condition=AutomationCondition.eager(),
        ),
        "test_data": AssetOut(
            automation_condition=AutomationCondition.eager(),
        ),
    }
)
def split_train_test(df_input):
    train_data, test_data = train_test_split(df_input, test_size=0.2, random_state=42, stratify=df_input["has_churned"])
    return train_data, test_data


@asset(group_name=group_name, automation_condition=AutomationCondition.eager())
def trained_model(train_data, test_data):
    # Dynamically select columns to impute
    columns_to_impute = [col for col in train_data.columns if col.startswith("n_case") or col.startswith("days_since_last_case")]

    # Define imputation for numeric columns
    imputer = SimpleImputer(strategy="constant", fill_value=0)

    # One-hot encoding for categorical columns
    categorical_transformer = OneHotEncoder(handle_unknown="ignore")

    # ColumnTransformer: numeric imputation + categorical processing
    preprocessor = ColumnTransformer(
        transformers=[
            ("num_imputer", imputer, columns_to_impute),
            (
                "one_hot_encoder",
                categorical_transformer,
                ["smartphone_brand"],
            ),
        ],
        remainder="passthrough",
        verbose_feature_names_out=False,
    )

    # Define the complete pipeline
    pipeline = Pipeline(
        [
            ("preprocessing", preprocessor),
            ("classifier", XGBClassifier()),
        ]
    )

    # Separate target variable
    y_train = train_data.pop("has_churned")
    train_data = train_data.set_index("rating_account_id")
     
    y_test = test_data.pop("has_churned")
    test_data = test_data.set_index("rating_account_id")

    # Train model
    trained_model = pipeline.fit(train_data, y_train)

    # Predicted labels
    y_pred = trained_model.predict(test_data)
    # Predicted probabilities
    y_prob = trained_model.predict_proba(test_data)[:, 1]
    # Metrics calculation
    metrics = {
        "precision_score": float(precision_score(y_test, y_pred)),
        "recall_score": float(recall_score(y_test, y_pred)),
        "f1_score": float(f1_score(y_test, y_pred)),
        "roc_auc_score": float(roc_auc_score(y_test, y_prob)),
    }
    return Output(value=trained_model, metadata={**metrics})

