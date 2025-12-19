from dagster import Definitions
from .assets import dbt_mastery_assets
from .resources import dbt_resource

defs = Definitions(
    assets=[dbt_mastery_assets],
    resources={
        "dbt": dbt_resource,
    },
)