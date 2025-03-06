from dagster_sdf import SdfCliResource
from pathlib import Path
import json
import glob
import os

import yaml

from genesis_bots.core.logging_config import logger

sdf_workspace_dir = Path.cwd().joinpath('sdf_genesis')
target_dir = sdf_workspace_dir.joinpath("sdftarget-thread-123")
target_dir.mkdir(parents=True, exist_ok=True)
log_file = str(target_dir.joinpath("log.json"))

sdf_cli = SdfCliResource(workspace_dir=sdf_workspace_dir, target_dir=target_dir)

assets = sdf_cli.cli(["compile", "--save", "info-schema", "--log-level", "info",
                      "--log-file", log_file,
                      #"--target-dir", str(target_dir),
                      "--query", """
SELECT domain_id FROM tech__innovation_essentials.cybersyn.domain_characteristics WHERE domain_id ILIKE '%.ai'  AND relationship_type = 'successful_http_response_status'  AND value = 'true'  AND relationship_end_date IS NULL
"""]).stream()

try:
    assets_list = []
    for asset in assets:
        #logger.info(asset)
        assets_list.append(asset)
    logger.info(assets_list)

    sdf_dagster_out_dir = sdf_workspace_dir.joinpath('sdf_dagster_out')
    for subdir in os.listdir(sdf_dagster_out_dir):
        query_sdf_file = glob.glob(str(sdf_dagster_out_dir.joinpath(subdir, 'sdftarget', 'dbg', 'table', 'sdf_genesis', 'pub', 'query.sdf.yml')))
        if query_sdf_file:
            with open(query_sdf_file[0], 'r') as f:
                query_sdf_data = yaml.safe_load(f)
                logger.info(query_sdf_data)
except Exception as e:
    logger.info(e)
    with open(log_file, 'r') as f:
        log_data = [json.loads(line) for line in f.readlines()]
        error_rows = [row for row in log_data if row["_ll"] == "ERROR"]
        logger.info(error_rows)

