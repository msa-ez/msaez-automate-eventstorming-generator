from ...mocks import gwt_worker_subgraph_inputs
from .....subgraphs import create_gwt_worker_subgraph, gwt_worker_id_context
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_gwt_worker_sub_graph(command_args):
    run_name = "run_gwt_worker_sub_graph"
    worker_id = 'dc95ecbe-b66a-4fad-91a3-71dd351ddcaf'

    try:

        run_subgraph = create_gwt_worker_subgraph()
        gwt_worker_id_context.set(worker_id)
        result: State = run_subgraph(gwt_worker_subgraph_inputs)

        completed_gwt = result.subgraphs.createGwtGeneratorByFunctionModel.worker_generations.get(worker_id)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(completed_gwt, f"{run_name}_completed_gwt")
        TerminalHelper.save_dict_to_temp_file({
            "elements": [completed_gwt.command_to_replace],
            "relations": [],
            "logs": result.outputs.logs,
        }, f"{run_name}_completed_value")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
