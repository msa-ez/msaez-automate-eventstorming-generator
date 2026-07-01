from ...mocks import policy_actions_worker_subgraph_inputs
from .....subgraphs import create_policy_actions_worker_subgraph, policy_actions_worker_id_context
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_policy_actions_worker_subgraph(command_args):
    run_name = "run_policy_actions_worker_subgraph"
    worker_id = 'edf5d875-d5ea-49b0-ae2c-b5d110786aaa'

    try:

        run_subgraph = create_policy_actions_worker_subgraph()
        policy_actions_worker_id_context.set(worker_id)
        result: State = run_subgraph(policy_actions_worker_subgraph_inputs)

        completed_policy_actions = result.subgraphs.createPolicyActionsByFunctionModel.worker_generations.get(worker_id)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_policy_actions, f"{run_name}_completed_policy_actions")
        TerminalHelper.save_dict_to_temp_file({
            "extractedPolicies": [completed_policy_actions.extractedPolicies],
            "logs": result.outputs.logs,
        }, f"{run_name}_extractedPolicies")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
