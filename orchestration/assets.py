from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from .constants import DBT_MANIFEST_PATH

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def dbt_mastery_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
