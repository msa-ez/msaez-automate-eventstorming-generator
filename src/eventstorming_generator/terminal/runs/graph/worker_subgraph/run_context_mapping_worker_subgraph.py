from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State, ContextMappingGenerationState
from ....commons.graph import execute_context_mapping_worker_subgraph

def run_context_mapping_worker_subgraph(command_args):
    run_name = "run_context_mapping_worker_subgraph"

    try:

        output = execute_context_mapping_worker_subgraph()
        state: State = output["state"]
        completed_context_mapping: ContextMappingGenerationState = output["completed_context_mapping"]

        RunHelper.check_error_logs_from_state(state, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_context_mapping, f"{run_name}_completed_context_mapping")
        TerminalHelper.save_dict_to_temp_file({
            "created_context_mappings": [m.model_dump() for m in completed_context_mapping.created_context_mappings],
            "logs": state.outputs.logs,
        }, f"{run_name}_created_context_mappings")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
