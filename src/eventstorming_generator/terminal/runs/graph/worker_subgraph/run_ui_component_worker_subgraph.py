from ...mocks import ui_component_worker_subgraph_inputs
from .....subgraphs import create_ui_component_worker_subgraph, ui_component_worker_id_context
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_ui_component_worker_subgraph(command_args):
    run_name = "run_ui_component_worker_subgraph"
    worker_id = '2b73cf50-9b29-4f6f-866a-c59dab79f1e8'

    try:

        run_subgraph = create_ui_component_worker_subgraph()
        ui_component_worker_id_context.set(worker_id)
        result: State = run_subgraph(ui_component_worker_subgraph_inputs)

        completed_ui_component = result.subgraphs.createUiComponentsModel.worker_generations.get(worker_id)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_ui_component, f"{run_name}_completed_ui_component")
        TerminalHelper.save_dict_to_temp_file({
            "ui_replace_actions": [completed_ui_component.ui_replace_actions],
            "logs": result.outputs.logs,
        }, f"{run_name}_ui_replace_actions")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
