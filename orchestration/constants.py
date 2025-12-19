import os
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__),joinpath("..", "..").resolve()
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

