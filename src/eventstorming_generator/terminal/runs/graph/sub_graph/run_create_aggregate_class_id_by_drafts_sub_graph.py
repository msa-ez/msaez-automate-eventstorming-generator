from ...mocks import create_aggregate_class_id_by_drafts_sub_graph_inputs
from .....subgraphs import create_aggregate_class_id_by_drafts_subgraph
from ....terminal_helper import TerminalHelper
from ...run_helper import RunHelper
from .....utils import LoggingUtil
from .....models import State

def run_create_aggregate_class_id_by_drafts_sub_graph(command_args):
    run_name = "run_create_aggregate_class_id_by_drafts_sub_graph"

    try:

        run_subgraph = create_aggregate_class_id_by_drafts_subgraph()
        result: State = run_subgraph(create_aggregate_class_id_by_drafts_sub_graph_inputs)

        RunHelper.check_error_logs_from_state(result, run_name)
        TerminalHelper.save_dict_to_temp_file({
            "esValue": result.outputs.esValue,
            "logs": result.outputs.logs,
            "totalSeconds": result.subgraphs.createAggregateClassIdByDraftsModel.total_seconds
        }, run_name)
        RunHelper.save_es_summarize_result_to_temp_file(result.outputs.esValue, run_name)

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise
