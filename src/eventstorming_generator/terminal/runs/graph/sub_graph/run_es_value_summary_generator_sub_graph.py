from ...mocks import es_value_summary_generator_sub_graph_inputs
from .....subgraphs import create_es_value_summary_generator_subgraph
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_es_value_summary_generator_sub_graph(command_args):
    run_name = "run_es_value_summary_generator_sub_graph"

    try:

        run_subgraph = create_es_value_summary_generator_subgraph()
        result: State = run_subgraph(es_value_summary_generator_sub_graph_inputs)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file(result.subgraphs.esValueSummaryGeneratorModel, run_name)
        
    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
