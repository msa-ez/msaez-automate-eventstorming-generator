from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State, DraftGenerationState
from ....commons.graph import execute_draft_worker_subgraph

def run_draft_worker_subgraph(command_args):
    run_name = "run_draft_worker_subgraph"

    try:

        output = execute_draft_worker_subgraph()
        state: State = output["state"]
        completed_draft: DraftGenerationState = output["completed_draft"]

        RunHelper.check_error_logs_from_state(state, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_draft, f"{run_name}_completed_draft")
        TerminalHelper.save_dict_to_temp_file({
            "created_draft": completed_draft.created_draft,
            "logs": state.outputs.logs,
        }, f"{run_name}_created_draft")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
