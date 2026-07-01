from ...mocks import class_id_worker_subgraph_inputs
from .....subgraphs import create_class_id_worker_subgraph, class_id_worker_id_context
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_class_id_worker_subgraph(command_args):
    run_name = "run_class_id_worker_subgraph"
    worker_id = '9439b794-f494-4755-93d1-7035b10c7f64'
    
    try:

        run_subgraph = create_class_id_worker_subgraph()
        class_id_worker_id_context.set(worker_id)
        result: State = run_subgraph(class_id_worker_subgraph_inputs)

        completed_class_id = result.subgraphs.createAggregateClassIdByDraftsModel.worker_generations.get(worker_id)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_class_id, f"{run_name}_completed_class_id")
        TerminalHelper.save_dict_to_temp_file({
            "created_actions": completed_class_id.created_actions,
            "logs": result.outputs.logs,
        }, f"{run_name}_created_actions")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
