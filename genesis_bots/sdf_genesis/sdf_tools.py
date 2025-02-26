from dagster_sdf import SdfCliResource
from pathlib import Path
from genesis_bots.core.logging_config import logger

sdf_workspace_dir = Path.cwd().joinpath('sdf_workspaces', 'sdf_genesis')  # Set to the current working directory plus '/sdf_workspaces/sdf_genesis' at runtime
sdf_cli = SdfCliResource(workspace_dir=sdf_workspace_dir)#, environment=environment)

def run_sdf_command(command: list[str]) -> list[str]:
    assets = sdf_cli.cli(command).stream()
    assets_list = []
    for asset in assets:
        logger.info(asset)
        assets_list.append(asset)
    return assets_list

def create_or_replace_model(model_path: str, model_name: str, sql_str: str):
    try:
        model_file_path = Path.cwd().joinpath('sdf_workspaces/sdf_genesis', 'models', model_path, model_name + '.sql')
        model_file_path.parent.mkdir(parents=True, exist_ok=True)
        with model_file_path.open('w') as model_file:
            model_file.write(sql_str)
        compile_output = run_sdf_command(["compile", "--save", "info-schema", "--log-level", "error", str(model_file_path)])
        logger.info(compile_output)
        return f"Model {model_path}/{model_name}.sql produced this output from sdf compile:\n" + str(compile_output)
    except Exception as e:
        return str(e)

ret = create_or_replace_model('my_data/matt_local_workspace', 'main_2', """
select 'Hello cruel world!' as message,
       'Goodbye World!' as message2,
       'it is over' as message3,
       'Goodbye, goodbye, goodbye' as message4
""")
logger.info(ret)

