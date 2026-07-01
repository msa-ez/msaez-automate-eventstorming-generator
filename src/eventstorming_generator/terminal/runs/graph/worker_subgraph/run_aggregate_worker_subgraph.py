from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State, AggregateGenerationState
from ....commons.graph import execute_aggregate_worker_subgraph

def run_aggregate_worker_subgraph(command_args):
    run_name = "run_aggregate_worker_subgraph"
    try:

        output = execute_aggregate_worker_subgraph()
        state: State = output["state"]
        completed_aggregate: AggregateGenerationState = output["completed_aggregate"]

        RunHelper.check_error_logs_from_state(state, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_aggregate, f"{run_name}_completed_aggregate")
        TerminalHelper.save_dict_to_temp_file({
            "created_actions": completed_aggregate.created_actions,
            "logs": state.outputs.logs,
        }, f"{run_name}_created_actions")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
