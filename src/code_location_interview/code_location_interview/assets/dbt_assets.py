import os

from dagster import OpExecutionContext
from dagster_dbt import DagsterDbtTranslatorSettings, DbtCliResource, DbtProject
from dagster_dbt.asset_decorator import dbt_assets
from shared_library.orchestration.dbt_translator import (
    build_DbtTranslator,
    process_dbt_assets,
)

from code_location_interview.resources import (
    resource_defs_by_deployment_name,
)
from code_location_interview.resources.sql_asset_keys import duckdb_bar_warehouse_name

# TODO update to our needs https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli_data_eng/assets/dbt_assets.py
dbt_target_schema = os.environ.get("ASCII_WAREHOUSE_SCHEMA", "ascii")

dagster_dbt_translator = build_DbtTranslator(
    duckdb_bar_warehouse_name, dbt_target_schema
)(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True, enable_code_references=True
    )
)

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "dev")
resource_defs = resource_defs_by_deployment_name[deployment_name]
dbt_cli: DbtCliResource = resource_defs["dbt"]
dbt_project: DbtProject = DbtProject(
    project_dir=dbt_cli.project_dir,
    target=dbt_cli.target,
    state_path=dbt_cli.state_path,
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    exclude="tag:long_running_test",
    dagster_dbt_translator=dagster_dbt_translator,
)
def unpartitioned_assets(context: OpExecutionContext, dbt: DbtCliResource):
    yield from process_dbt_assets(
        context=context, dbt2=dbt, dagster_dbt_translator2=dagster_dbt_translator
    )


# @dbt_assets(
#     manifest=Path(DBT_MANIFEST_PATH),
#     io_manager_key="dwh_oracle_io_manager",
#     select="tag:daily",
#     partitions_def=daily_partitions,
#     dagster_dbt_translator=dagster_dbt_translator,
# )
# def daily_partitioned_assets(context: OpExecutionContext, dbt: DbtCliResource):
#     yield from process_dbt_assets(context=context, dbt2=dbt)
#     yield from dbt.cli(["test"], context=context).stream()
