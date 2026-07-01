from ...mocks import command_actions_worker_subgraph_inputs
from .....subgraphs import create_command_actions_worker_subgraph, command_actions_worker_id_context
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_command_actions_worker_subgraph(command_args):
    run_name = "run_command_actions_worker_subgraph"
    worker_id = 'bd0ee73e-e6cb-46a4-b8c7-4f9c8865558e'

    try:

        run_subgraph = create_command_actions_worker_subgraph()
        command_actions_worker_id_context.set(worker_id)
        result: State = run_subgraph(command_actions_worker_subgraph_inputs)

        completed_command_actions = result.subgraphs.createCommandActionsByFunctionModel.worker_generations.get(worker_id)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_command_actions, f"{run_name}_completed_command_actions")
        TerminalHelper.save_dict_to_temp_file({
            "created_actions": completed_command_actions.created_actions,
            "logs": result.outputs.logs,
        }, f"{run_name}_created_actions")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
