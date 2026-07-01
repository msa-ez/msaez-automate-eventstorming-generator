from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil, ListUtil
from .....models import State
from .....config import Config
from ....commons.graph import execute_create_policy_actions_by_function_sub_graph
from .....constants import RG

def run_create_policy_actions_by_function_sub_graph(command_args):
    run_name = "run_create_policy_actions_by_function_sub_graph"
    after_stop_node = RG.CREATE_POLICY_ACTIONS

    try:

        db_type = ListUtil.get_safely(command_args, 2, "memory")
        Config.set_db_type(db_type)

        state: State = execute_create_policy_actions_by_function_sub_graph()
        RunHelper.save_state_by_after_stop_node(after_stop_node, state, run_name)
        
    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise