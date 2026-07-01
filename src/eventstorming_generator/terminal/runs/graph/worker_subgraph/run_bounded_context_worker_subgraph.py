from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State, BoundedContextGenerationState
from ....commons.graph import execute_bounded_context_worker_subgraph

def run_bounded_context_worker_subgraph(command_args):
    run_name = "run_bounded_context_worker_subgraph"

    try:

        output = execute_bounded_context_worker_subgraph()
        state: State = output["state"]
        completed_bounded_context: BoundedContextGenerationState = output["completed_bounded_context"]

        RunHelper.check_error_logs_from_state(state, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_bounded_context, f"{run_name}_completed_bounded_context")
        TerminalHelper.save_dict_to_temp_file({
            "created_bounded_contexts": completed_bounded_context.created_bounded_contexts,
            "logs": state.outputs.logs,
        }, f"{run_name}_created_bounded_contexts")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
