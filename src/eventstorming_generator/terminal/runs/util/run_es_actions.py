from typing import List

from ....models import ActionModel, EsValueModel
from ....utils import EsActionsUtil, EsAliasTransManager, LoggingUtil, ListUtil
from ..mocks import user_id, project_id, actions_collection, total_actions
from ...terminal_helper import TerminalHelper
from ..run_helper import RunHelper
from .mock_actions_builder import MockActionBuilder

def run_es_actions(command_args):
    run_type = ListUtil.get_safely(command_args, 1, None)

    if run_type == "actions_collection":
        run_es_actions_with_actions_collection(command_args)
    elif run_type == "total_actions":
        run_es_actions_with_total_actions(command_args)
    elif run_type == "mocked_actions":
        run_es_actions_with_mocked_actions(command_args)
    else:
        print(f"유효하지 않은 실행 타입입니다. {run_type}")
        return

def run_es_actions_with_actions_collection(command_args):
    run_name = "run_es_actions_with_actions_collection"

    try:

        es_value = EsValueModel()
        alias_trans_manager = EsAliasTransManager(es_value)

        result = es_value
        for actions in actions_collection:
            uuid_actions = alias_trans_manager.trans_to_uuid_in_actions(actions)
            result = EsActionsUtil.apply_actions(result, uuid_actions, user_id, project_id)
        

        target_ui_element = None
        for element in result["elements"].values():
            if element["name"] == "LoanBookUI":
                target_ui_element = element
                break
        
        if target_ui_element:
            result = EsActionsUtil.apply_actions(result, [
                ActionModel(
                    objectType="UI",
                    type="update",
                    ids={"uiId": target_ui_element["id"]},
                    args={"runTimeTemplateHtml": "Updated Template HTML"}
                )
            ], user_id, project_id)
        

        TerminalHelper.save_dict_to_temp_file(result, run_name)
        RunHelper.save_es_summarize_result_to_temp_file(result, run_name)

    except Exception as e:
        LoggingUtil.exception(run_name, "실행 실패", e)
        TerminalHelper.save_dict_to_temp_file(
            {
                "error": str(e)
            },
            f"{run_name}_error",
        )
        raise

def run_es_actions_with_total_actions(command_args):
    run_name = "run_es_actions_with_total_actions"

    try:

        result = EsActionsUtil.apply_actions(total_actions["esValue"], total_actions["actions"], user_id, project_id) 
        TerminalHelper.save_dict_to_temp_file(result, run_name)
        RunHelper.save_es_summarize_result_to_temp_file(result, run_name)

    except Exception as e:
        LoggingUtil.exception(run_name, "실행 실패", e)
        TerminalHelper.save_dict_to_temp_file(
            {
                "error": str(e)
            },
            f"{run_name}_error",
        )
        raise

def run_es_actions_with_mocked_actions(command_args):
    run_name = "run_es_actions_with_mocked_actions"

    try:

        es_value = EsValueModel()
        alias_trans_manager = EsAliasTransManager(es_value)

        mocked_actions = [_make_mocked_actions([[6, 5, 7]] * 4)]

        result = es_value
        for actions in mocked_actions:
            uuid_actions = alias_trans_manager.trans_to_uuid_in_actions(actions)
            result = EsActionsUtil.apply_actions(result, uuid_actions, user_id, project_id)
        
        TerminalHelper.save_dict_to_temp_file(result, run_name)
        RunHelper.save_es_summarize_result_to_temp_file(result, run_name)

    except Exception as e:
        LoggingUtil.exception(run_name, "실행 실패", e)
        TerminalHelper.save_dict_to_temp_file(
            {
                "error": str(e)
            },
            f"{run_name}_error",
        )
        raise

def _make_mocked_actions(mock_info: List[List[int]]) -> List[ActionModel]:
    builder = MockActionBuilder()
    for bc_index, aggregates in enumerate(mock_info, 1):
        builder.with_bounded_context(bc_index)
        for agg_index, command_count in enumerate(aggregates, 1):
            builder.with_aggregate(agg_index)
            builder.with_command_event_pairs(command_count)
    return builder.build()