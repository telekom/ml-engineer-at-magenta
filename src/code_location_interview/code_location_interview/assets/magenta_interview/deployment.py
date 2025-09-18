"""
Based on:
https://github.com/neurospaceio/dagster-deepdive-mlops-demo/blob/main/mlops_demo/mlops_demo/deployment.py
"""

import polars as pl
from dagster import asset, file_relative_path, AssetExecutionContext, Config, Failure,AutomationCondition
from sklearn.ensemble import RandomForestClassifier
import pickle
from datetime import datetime

group_name = "deployment"

@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def model_candidates(context: AssetExecutionContext, trained_model) -> list[str]:
    root =  file_relative_path(__file__, "../../../../../")
    trained_models_folder = f"{root}/trained_models"
    model_list_file_name = f"{trained_models_folder}/list-of-models.txt"
    model_path = f"{trained_models_folder}/model-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.pickle"
    
    # Save model file
    pickle.dump(trained_model, open(model_path, "wb+"))

    # Update model candidates
    with open(model_list_file_name, "a+") as f:
        f.write(model_path + "\n")

    with open(model_list_file_name, "r") as f:
        models = f.read().splitlines()

    context.add_asset_metadata({
        "list-of-models": models
    })

    return models

class DeployedModelConfiguration(Config):
    model_path: str

@asset(
    group_name=group_name
)
def deployed_model(model_candidates: list[str], config: DeployedModelConfiguration):

    if config.model_path not in model_candidates:
        raise Failure("The requested model does not exist")
    
    with open(config.model_path, "rb") as f:
        model = pickle.load(f)

    return model
    