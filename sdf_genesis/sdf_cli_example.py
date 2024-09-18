from dagster_sdf import SdfCliResource, SdfWorkspace, sdf_assets
import polars as pl
from pathlib import Path

sdf_workspace_dir = Path.cwd().joinpath('sdf_workspaces', 'sdf_genesis')  # Set to the current working directory plus '/sdf_workspaces/sdf_genesis' at runtime
#target_dir = sdf_workspace_dir.joinpath("sdf_dagster_out2") #sdf_out")
#environment = "dbg"

#sdf_cli = SdfCliResource(workspace_dir=sdf_workspace_dir, target_dir=target_dir, environment=environment)
sdf_cli = SdfCliResource(workspace_dir=sdf_workspace_dir)#, environment=environment)

#assets = sdf_cli.cli(["compile"], target_dir=target_dir, environment=environment, context=None).stream()
# assets = sdf_cli.cli(["compile"])#, environment=environment, context=None).stream()

# assets_list = []
# for asset in assets:
#     print(asset)
#     assets_list.append(asset)

# print(len(assets_list))

#assets = sdf_cli.cli(["run", "--save", "info-schema"], target_dir=target_dir, environment=environment, context=None).stream()
assets = sdf_cli.cli(["run", "--save", "info-schema"]).stream()#, target_dir=target_dir, environment=environment, context=None).stream()
assets_list = []
for asset in assets:
    print(asset)
    assets_list.append(asset)

print(len(assets_list))