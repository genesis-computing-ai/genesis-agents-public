from dagster_sdf import SdfCliResource
from pathlib import Path
import json

sdf_workspace_dir = Path.cwd().joinpath('sdf_workspaces', 'sdf_genesis')  
target_dir = sdf_workspace_dir.joinpath("sdftarget-thread-123")
target_dir.mkdir(parents=True, exist_ok=True)
log_file = str(target_dir.joinpath("log.json"))

sdf_cli = SdfCliResource(workspace_dir=sdf_workspace_dir, target_dir=target_dir)

assets = sdf_cli.cli(["compile", "--save", "info-schema", "--log-level", "info", 
                      "--log-file", log_file,
                      #"--target-dir", str(target_dir),
                      "--query", """
SELECT domain_id2 FROM tech__innovation_essentials.cybersyn.domain_characteristics WHERE domain_id ILIKE '%.ai'  AND relationship_type = 'successful_http_response_status'  AND value = 'true'  AND relationship_end_date IS NULL
"""]).stream()

try:
    assets_list = []
    for asset in assets:
        #print(asset)
        assets_list.append(asset)
    print(assets_list)
except Exception as e:
    print(e)
    with open(log_file, 'r') as f:
        log_data = [json.loads(line) for line in f.readlines()]
        error_rows = [row for row in log_data if row["_ll"] == "ERROR"]
        print(error_rows)

