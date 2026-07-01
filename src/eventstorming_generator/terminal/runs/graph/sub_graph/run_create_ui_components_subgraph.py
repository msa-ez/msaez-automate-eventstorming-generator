from ...mocks import create_ui_components_subgraph_inputs
from .....subgraphs import create_ui_components_subgraph
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_create_ui_components_subgraph(command_args):
    run_name = "run_create_ui_components_subgraph"

    try:

        run_subgraph = create_ui_components_subgraph()
        result: State = run_subgraph(create_ui_components_subgraph_inputs)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file({
            "esValue": result.outputs.esValue,
            "logs": result.outputs.logs,
            "totalSeconds": result.subgraphs.createUiComponentsModel.total_seconds
        }, run_name)
        RunHelper.save_es_summarize_result_to_temp_file(result.outputs.esValue, run_name)
        
    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
