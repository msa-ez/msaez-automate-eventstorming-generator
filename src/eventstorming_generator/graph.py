from langgraph.graph import StateGraph, START

from eventstorming_generator.models import ActionModel, State
from eventstorming_generator.utils import JsonUtil, EsActionsUtil, LogUtil, self_dict
from eventstorming_generator.utils.job_utils import JobUtil
from eventstorming_generator.subgraphs import create_bounded_context_by_functions_subgraph, create_context_mapping_subgraph, create_draft_by_function_subgraph, create_aggregate_by_functions_subgraph, create_aggregate_class_id_by_drafts_subgraph, create_element_names_by_draft_sub_graph, create_command_actions_by_function_subgraph, create_policy_actions_by_function_subgraph, create_gwt_generator_by_function_subgraph, create_ui_components_subgraph
from eventstorming_generator.constants import RG, RESUME_NODES, REQUEST_TYPES


def resume_from_root_graph(state: State):
    if not state.inputs.jobId or not JobUtil.is_valid_job_id(state.inputs.jobId):
        LogUtil.add_error_log(state, f"[ROOT_GRAPH] Invalid job ID provided: '{state.inputs.jobId}'")
        return RG.COMPLETE

    if state.outputs.lastCompletedRootGraphNode:
        if state.outputs.lastCompletedRootGraphNode in RG.__dict__.values():
            LogUtil.add_info_log(state, f"[ROOT_GRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedRootGraphNode}'")
            return state.outputs.lastCompletedRootGraphNode
        else:
            LogUtil.add_error_log(state, f"[ROOT_GRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedRootGraphNode}'")
            return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Starting new event storming generation process")
    state.outputs.currentProgressCount = 0
    if state.inputs.requestType == REQUEST_TYPES.FROM_REQUIREMENTS:
        state.outputs.totalProgressCount = getTotalProgressCount(REQUEST_TYPES.FROM_REQUIREMENTS)
        return RG.CREATE_BOUNDED_CONTEXTS
    elif state.inputs.requestType == REQUEST_TYPES.FROM_DRAFT:
        state.outputs.totalProgressCount = getTotalProgressCount(REQUEST_TYPES.FROM_DRAFT)
        return RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE
    else:
        LogUtil.add_error_log(state, f"[ROOT_GRAPH] Unsupported request type: '{state.inputs.requestType}'")
        return RG.COMPLETE

def route_after_create_bounded_contexts(state: State):
    if state.subgraphs.createBoundedContextByFunctionsModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Bounded context creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_BOUNDED_CONTEXTS:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Bounded context creation completed, proceeding to context mapping")
    return RG.CREATE_CONTEXT_MAPPING

def route_after_create_context_mapping(state: State):
    if state.subgraphs.createContextMappingModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Context mapping creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_CONTEXT_MAPPING:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Context mapping creation completed, proceeding to draft creation")
    return RG.CREATE_DRAFT_BY_FUNCTION

def route_after_create_draft(state: State):
    if state.subgraphs.createDraftByFunctionModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Draft creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_DRAFT_BY_FUNCTION:
        return RG.COMPLETE

    LogUtil.add_info_log(state, "[ROOT_GRAPH] Draft creation completed, proceeding to bounded context creation")
    return RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE

def create_bounded_contexts_to_es_value(state: State):
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Starting bounded context creation process")
    draft = state.inputs.draft

    try :
        context_count = len(draft.structures)
        LogUtil.add_info_log(state, f"[ROOT_GRAPH] Processing {context_count} bounded contexts for creation")

        for idx, structure in enumerate(draft.structures, 1):
            bc_name = structure.boundedContextName
            bc_alias = structure.boundedContextAlias
            bc_id = f"bc_{bc_name}"
            bc_description = draft.metadatas.boundedContextRequirements.get(bc_name, "")

            actions = [
                ActionModel(
                    objectType="BoundedContext",
                    type="create",
                    ids={
                        "boundedContextId": bc_id
                    },
                    args={
                        "boundedContextName": bc_name,
                        "boundedContextAlias": bc_alias,
                        "description": bc_description
                    }
                )
            ]
            
            state.outputs.esValue = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(), 
                actions, 
                state.inputs.ids.uid, 
                state.inputs.ids.projectId
            )

    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[ROOT_GRAPH] Failed to create bounded contexts", e)
        state.outputs.lastCompletedRootGraphNode = RG.CREATE_AGGREGATES
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_AGGREGATES.COMPLETE
        state.subgraphs.createAggregateByFunctionsModel.is_failed = True
        return state

    LogUtil.add_info_log(state, f"[ROOT_GRAPH] Bounded context creation completed successfully.")
    state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
    return state

def route_after_create_aggregates(state: State):
    if state.subgraphs.createAggregateByFunctionsModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Aggregate creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_AGGREGATES:
        return RG.COMPLETE

    LogUtil.add_info_log(state, "[ROOT_GRAPH] Aggregate creation completed, proceeding to class ID generation")
    return RG.CREATE_CLASS_ID

def route_after_create_class_id(state: State):
    if state.subgraphs.createAggregateClassIdByDraftsModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Class ID generation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_CLASS_ID:
        return RG.COMPLETE

    LogUtil.add_info_log(state, "[ROOT_GRAPH] Class ID generation completed, proceeding to command actions")
    return RG.CREATE_ELEMENT_NAMES

def route_after_create_element_names(state: State):
    if state.subgraphs.createElementNamesByDraftsModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Element names creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_ELEMENT_NAMES:
        return RG.COMPLETE

    LogUtil.add_info_log(state, "[ROOT_GRAPH] Element names creation completed, proceeding to command actions")
    return RG.CREATE_COMMAND_ACTIONS

def route_after_create_command_actions(state: State):
    if state.subgraphs.createCommandActionsByFunctionModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Command actions creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_COMMAND_ACTIONS:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Command actions creation completed, proceeding to policy actions")
    return RG.CREATE_POLICY_ACTIONS

def route_after_create_policy_actions(state: State):
    if state.subgraphs.createPolicyActionsByFunctionModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Policy actions creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_POLICY_ACTIONS:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] Policy actions creation completed, proceeding to GWT generation")
    return RG.CREATE_GWT

def route_after_create_gwt(state: State):
    if state.subgraphs.createGwtGeneratorByFunctionModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] GWT generation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_GWT:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] GWT generation completed, proceeding to completion")
    return RG.CREATE_UI_COMPONENTS

def route_after_create_ui_components(state: State):
    if state.subgraphs.createUiComponentsModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] UI components creation failed, terminating process")
        return RG.COMPLETE
    if state.inputs.after_stop_node == RG.CREATE_UI_COMPONENTS:
        return RG.COMPLETE
    
    LogUtil.add_info_log(state, "[ROOT_GRAPH] UI components creation completed, proceeding to completion")
    return RG.COMPLETE

def complete(state: State):
    LogUtil.add_info_log(state, "[ROOT_GRAPH] complete 함수 호출됨 - 완료 처리 시작")
    
    # 오류가 발생한 경우에는 향후 작업 큐에서 다시 재수행을 하기 위해서 COMPLETE 처리를 하지 않음
    if state.subgraphs.createBoundedContextByFunctionsModel.is_failed or \
       state.subgraphs.createContextMappingModel.is_failed or \
       state.subgraphs.createDraftByFunctionModel.is_failed or \
       state.subgraphs.createAggregateByFunctionsModel.is_failed or \
       state.subgraphs.createAggregateClassIdByDraftsModel.is_failed or \
       state.subgraphs.createCommandActionsByFunctionModel.is_failed or \
       state.subgraphs.createPolicyActionsByFunctionModel.is_failed or \
       state.subgraphs.createGwtGeneratorByFunctionModel.is_failed:
        LogUtil.add_error_log(state, "[ROOT_GRAPH] Event storming generation failed, terminating process")
        state.outputs.isFailed = True
        return state

    # 디버깅을 위한 Stop 노드가 설정된 상태에서는 향후 연계를 위해서 COMPLETE 처리를 하지 않음
    if state.inputs.after_stop_node:
        LogUtil.add_info_log(state, "[ROOT_GRAPH] after_stop_node가 설정되어 있어 완료 처리를 건너뜀")
        return state


    state.outputs.lastCompletedRootGraphNode = RG.COMPLETE
    state.outputs.isCompleted = True
    total_progress = state.outputs.totalProgressCount
    current_progress = state.outputs.currentProgressCount
    
    LogUtil.add_info_log(state, f"[ROOT_GRAPH] Event storming generation process completed successfully. Final progress: {current_progress}/{total_progress}")
    
    # 전체 state 업데이트
    JobUtil.update_job_to_firebase_fire_and_forget(state)
    
    # UI가 watch하는 isCompleted 경로에 명시적으로 업데이트 (AceBase 연결 불안정성 대비)
    LogUtil.add_info_log(state, "[ROOT_GRAPH] isCompleted 플래그를 AceBase에 업데이트 중...")
    JobUtil.update_job_is_completed_fire_and_forget(state, True)
    LogUtil.add_info_log(state, "[ROOT_GRAPH] isCompleted 플래그 업데이트 완료")

    return state


def compile_main_graph():
    graph_builder = StateGraph(State)

    graph_builder.add_node(RG.CREATE_BOUNDED_CONTEXTS, create_bounded_context_by_functions_subgraph())
    graph_builder.add_node(RG.CREATE_CONTEXT_MAPPING, create_context_mapping_subgraph())
    graph_builder.add_node(RG.CREATE_DRAFT_BY_FUNCTION, create_draft_by_function_subgraph())
    graph_builder.add_node(RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE, create_bounded_contexts_to_es_value)
    graph_builder.add_node(RG.CREATE_AGGREGATES, create_aggregate_by_functions_subgraph())
    graph_builder.add_node(RG.CREATE_CLASS_ID, create_aggregate_class_id_by_drafts_subgraph())
    graph_builder.add_node(RG.CREATE_ELEMENT_NAMES, create_element_names_by_draft_sub_graph())
    graph_builder.add_node(RG.CREATE_COMMAND_ACTIONS, create_command_actions_by_function_subgraph())
    graph_builder.add_node(RG.CREATE_POLICY_ACTIONS, create_policy_actions_by_function_subgraph())
    graph_builder.add_node(RG.CREATE_GWT, create_gwt_generator_by_function_subgraph())
    graph_builder.add_node(RG.CREATE_UI_COMPONENTS, create_ui_components_subgraph())
    graph_builder.add_node(RG.COMPLETE, complete)

    graph_builder.add_conditional_edges(
        START, resume_from_root_graph, 
        self_dict(
            RG.CREATE_BOUNDED_CONTEXTS,
            RG.CREATE_CONTEXT_MAPPING,
            RG.CREATE_DRAFT_BY_FUNCTION,
            RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE,
            RG.CREATE_AGGREGATES,
            RG.CREATE_CLASS_ID,
            RG.CREATE_ELEMENT_NAMES,
            RG.CREATE_COMMAND_ACTIONS,
            RG.CREATE_POLICY_ACTIONS,
            RG.CREATE_GWT,
            RG.CREATE_UI_COMPONENTS,
            RG.COMPLETE
        )
    )

    # A2A 전용 체인
    graph_builder.add_conditional_edges(
        RG.CREATE_BOUNDED_CONTEXTS, route_after_create_bounded_contexts, 
        self_dict(
            RG.CREATE_CONTEXT_MAPPING,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_CONTEXT_MAPPING, route_after_create_context_mapping,
        self_dict(
            RG.CREATE_DRAFT_BY_FUNCTION,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_DRAFT_BY_FUNCTION, route_after_create_draft,
        self_dict(
            RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE,
            RG.COMPLETE
        )
    )

    # 일반 체인
    graph_builder.add_edge(RG.CREATE_BOUNDED_CONTEXTS_TO_ES_VALUE, RG.CREATE_AGGREGATES)
    graph_builder.add_conditional_edges(
        RG.CREATE_AGGREGATES, route_after_create_aggregates,
        self_dict(
            RG.CREATE_CLASS_ID,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_CLASS_ID, route_after_create_class_id,
        self_dict(
            RG.CREATE_ELEMENT_NAMES,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_ELEMENT_NAMES, route_after_create_element_names,
        self_dict(
            RG.CREATE_COMMAND_ACTIONS,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_COMMAND_ACTIONS, route_after_create_command_actions,
        self_dict(
            RG.CREATE_POLICY_ACTIONS,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_POLICY_ACTIONS, route_after_create_policy_actions,
        self_dict(
            RG.CREATE_GWT,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_GWT, route_after_create_gwt, 
        self_dict(
            RG.CREATE_UI_COMPONENTS,
            RG.COMPLETE
        )
    )
    graph_builder.add_conditional_edges(
        RG.CREATE_UI_COMPONENTS, route_after_create_ui_components,
        self_dict(
            RG.COMPLETE
        )
    )

    return graph_builder.compile()

graph = compile_main_graph()


def getTotalProgressCount(request_type: str) -> int:
    if request_type == REQUEST_TYPES.FROM_REQUIREMENTS:
        return 11
    elif request_type == REQUEST_TYPES.FROM_DRAFT:
        return 8
    else:
        return 0
