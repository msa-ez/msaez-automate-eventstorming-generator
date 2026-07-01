from ...models import State
from ...constants import RG
from ..terminal_helper import TerminalHelper

class TerminalTestHelper:
    @staticmethod
    def test_state_by_after_stop_node(after_stop_node: str, previous_state: State, state: State) -> None:
        logs_to_check = state.outputs.logs

        error_logs = []
        for log in logs_to_check:
            if log.level == "error":
                error_logs.append(log)
        
        if len(error_logs) > 0:
            TerminalHelper.save_dict_to_temp_file(error_logs, f"error_logs_{after_stop_node}")
            raise RuntimeError(f"에러 로그가 있습니다: {", ".join([log.message for log in error_logs])}")

        if after_stop_node == RG.CREATE_BOUNDED_CONTEXTS:
            assert state.subgraphs.createBoundedContextByFunctionsModel.is_failed == False, "실패한 바운디드 컨텍스트가 있습니다"
            
            assert len(state.subgraphs.createBoundedContextByFunctionsModel.merged_bounded_contexts) > 0, "합병된 바운디드 컨텍스트가 없습니다"
            
        elif after_stop_node == RG.CREATE_CONTEXT_MAPPING:
            assert state.subgraphs.createContextMappingModel.is_failed == False, "실패한 컨텍스트 매핑이 있습니다"

            bc_count = len(previous_state.subgraphs.createBoundedContextByFunctionsModel.merged_bounded_contexts)

            assert state.inputs.draft.metadatas.boundedContextRequirements is not None and len(state.inputs.draft.metadatas.boundedContextRequirements) == bc_count, "추출된 바운디드 컨텍스트 요구사항이 없습니다"

            assert state.inputs.draft.metadatas.boundedContextRequirementIndexMapping is not None and len(state.inputs.draft.metadatas.boundedContextRequirementIndexMapping) == bc_count, "바운디드 컨텍스트 인덱스 매핑이 없습니다"

        elif after_stop_node == RG.CREATE_DRAFT_BY_FUNCTION:
            assert state.subgraphs.createDraftByFunctionModel.is_failed == False, "실패한 드래프트가 있습니다"

            bc_count = len(previous_state.subgraphs.createBoundedContextByFunctionsModel.merged_bounded_contexts)

            assert state.inputs.draft.metadatas.boundedContextRequirements is not None and len(state.inputs.draft.metadatas.boundedContextRequirements) == bc_count, "추출된 바운디드 컨텍스트 요구사항이 없습니다"

            assert state.inputs.draft.metadatas.boundedContextRequirementIndexMapping is not None and len(state.inputs.draft.metadatas.boundedContextRequirementIndexMapping) == bc_count, "바운디드 컨텍스트 인덱스 매핑이 없습니다"
        
        elif after_stop_node == RG.CREATE_AGGREGATES:
            assert state.subgraphs.createAggregateByFunctionsModel.is_failed == False, "실패한 어그리게이트가 있습니다"
        
        elif after_stop_node == RG.CREATE_COMMAND_ACTIONS:
            assert state.subgraphs.createCommandActionsByFunctionModel.is_failed == False, "실패한 명령 액션이 있습니다"
        
        elif after_stop_node == RG.CREATE_POLICY_ACTIONS:
            assert state.subgraphs.createPolicyActionsByFunctionModel.is_failed == False, "실패한 정책 액션이 있습니다"