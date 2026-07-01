import time

from ...terminal_helper import TerminalHelper
from ..run_helper import RunHelper
from ....utils import LoggingUtil, ListUtil
from ....models import State
from ....config import Config
from ...commons.graph import execute_main_graph_sequentially

def run_main_graph(command_args):
    run_name = "run_main_graph"
    target_directory = f".temp/run_main_graph_{time.time()}"

    try:
        
        request_type = ListUtil.get_safely(command_args, 1, "fromRequirements")
        requirements_type = ListUtil.get_safely(command_args, 2, "library_requirements")
        after_stop_node = ListUtil.get_safely(command_args, 3, "none")
        db_type = ListUtil.get_safely(command_args, 4, "memory")
        Config.set_db_type(db_type)


        save_index = 1
        def run_main_graph_callback(after_stop_node: str, previous_state: State, current_state: State):
            nonlocal save_index
            RunHelper.save_state_by_after_stop_node(
                after_stop_node, current_state, f"{run_name}_{save_index}_{after_stop_node}", target_directory
            )
            save_index += 1

        start_time = time.time()
        state: State = execute_main_graph_sequentially(
            request_type, requirements_type, after_stop_node, run_main_graph_callback
        )
        end_time = time.time()
        total_seconds = end_time - start_time


        RunHelper.save_state_by_after_stop_node(
            "complete", state, f"{run_name}_{save_index}_complete", target_directory
        )
        TerminalHelper.save_dict_to_temp_file({
            "totalSeconds": total_seconds
        }, f"{run_name}_{save_index}_total_seconds", target_directory)

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error", target_directory)
        raise
