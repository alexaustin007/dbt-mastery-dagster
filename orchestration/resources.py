import os
from dagster_dbt import DbtCliResource
from .constants import DBT_PROJECT_DIR

dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))