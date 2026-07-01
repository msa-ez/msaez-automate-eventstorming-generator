"""
단일 Aggregate에 대한 Command Actions 처리를 위한 워커 서브그래프

이 모듈은 하나의 Aggregate에 대해 preprocess -> generate -> postprocess -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional, List
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import CommandActionGenerationState, ActionModel, State, CreateCommandActionsByFunctionOutput
from ...utils import JsonUtil, ESValueSummarizeWithFilter, EsAliasTransManager, LogUtil, EsTraceUtil
from ...generators import CreateCommandActionsByFunction
from ...constants import ELEMENT_TYPES
from ...config import Config
from eventstorming_generator.utils.catchable_exceptions import CATCHABLE_EXCEPTIONS

# 스레드로부터 안전한 컨텍스트 변수 생성
command_actions_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[CommandActionGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createCommandActionsByFunctionModel
    worker_id = command_actions_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_command_actions(state: State) -> State:
    """
    단일 Aggregate에 대한 Command 액션 생성 전 전처리 작업 수행 (워커 전용)
    - 요약된 ES Value 생성
    - 요약된 정보가 토큰 제한을 초과하는지 확인하고 필요시 추가 요약
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] No current generation found in worker preprocess")
        return state

    aggregate_name = current_gen.target_aggregate_name
    bc_name = current_gen.target_bounded_context_name
    
    try:
        if current_gen.description:
            current_gen.description = EsTraceUtil.add_line_numbers_to_description(current_gen.description)

        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }

        # 요약된 ES Value 생성
        summarized_es_value = ESValueSummarizeWithFilter.get_summarized_es_value(
            es_value, [], EsAliasTransManager(es_value)
        )

        filtered_summarized_es_value = {
            "deletedProperties": summarized_es_value["deletedProperties"],
            "boundedContexts": []
        }
        for bounded_context_info in summarized_es_value["boundedContexts"]:
            if bounded_context_info["name"] != bc_name:
                continue

            filtered_bounded_context_info = {
                "id": bounded_context_info["id"],
                "name": bounded_context_info["name"],
                "aggregates": []
            }
            for aggregate_info in bounded_context_info["aggregates"]:
                if aggregate_info["name"] != aggregate_name:
                    continue

                filtered_bounded_context_info["aggregates"].append(aggregate_info)
            filtered_summarized_es_value["boundedContexts"].append(filtered_bounded_context_info)
        current_gen.summarized_es_value = filtered_summarized_es_value
        
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER] Preprocessing failed for aggregate '{aggregate_name}' in context '{bc_name}'", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_command_actions(state: State) -> State:
    """
    지정된 Aggregate에 대한 Command 액션 생성 실행 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] No current generation found in worker generate")
        return state
        
    aggregate_name = current_gen.target_aggregate_name
    bc_name = current_gen.target_bounded_context_name
    
    try:
        
        generator = CreateCommandActionsByFunction(
            model_name=Config.get_ai_model_light(),
            client={
                "inputs": {
                    "summarizedESValue": current_gen.summarized_es_value,
                    "description": current_gen.description,
                    "targetBoundedContextName": current_gen.target_bounded_context_name,
                    "targetAggregateName": current_gen.target_aggregate_name,
                    "eventNamesToGenerate": current_gen.extracted_element_names.event_names,
                    "commandNamesToGenerate": current_gen.extracted_element_names.command_names,
                    "readModelNamesToGenerate": current_gen.extracted_element_names.read_model_names
                },
                "preferredLanguage": state.inputs.preferedLanguage,
                "retryCount": current_gen.retry_count
            }
        )
        
        generator_output = generator.generate(
            bypass_cache=(current_gen.retry_count > 0),
            retry_count=current_gen.retry_count,
            extra_config_metadata={
                "job_id": state.inputs.jobId
            }
        )
        generator_result: CreateCommandActionsByFunctionOutput = generator_output["result"]
        
        actions = generator_result.commandActions + generator_result.eventActions + generator_result.readModelActions

        actionModels = [ActionModel(**action.model_dump()) for action in actions]
        if current_gen.extracted_element_names.event_names:
            missing_events = validate_required_events(current_gen.extracted_element_names.event_names, actionModels)
            missing_commands = validate_required_commands(current_gen.extracted_element_names.command_names, actionModels)
            missing_read_models = validate_required_read_models(current_gen.extracted_element_names.read_model_names, actionModels)
            if missing_events or missing_commands or missing_read_models:
                if current_gen.retry_count < state.subgraphs.createCommandActionsByFunctionModel.max_retry_count:
                    current_gen.retry_count += 1
                    LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER] Missing required elements for aggregate '{aggregate_name}': {missing_events} {missing_commands} {missing_read_models}. Retrying generation (attempt {current_gen.retry_count})")
                    return state
                else:
                    LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER] Missing required events for aggregate '{aggregate_name}': {missing_events}. Maximum retry count reached, proceeding with current result")
        
        current_gen.created_actions = actionModels
        
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER] Failed to generate command actions for aggregate '{aggregate_name}' in context '{bc_name}'", e)
        current_gen.retry_count += 1
    
    return state

def worker_postprocess_command_actions(state: State) -> State:
    """
    생성된 Command 액션 후처리 (워커 전용)
    - 유효한 액션만 필터링
    - 필요한 변환 작업 수행
    - UUID 변환 등
    - 워커에서는 ES 모델 업데이트 없이 액션만 저장하고 완료 표시
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] No current generation found in worker postprocess")
        return state
        
    aggregate_name = current_gen.target_aggregate_name
    bc_name = current_gen.target_bounded_context_name
    es_value = {
        "elements": state.outputs.esValue.elements,
        "relations": state.outputs.esValue.relations
    }
    
    try:
        # Refs 후처리
        try:
            EsTraceUtil.convert_refs_to_indexes(
                current_gen.created_actions, current_gen.original_description,
                current_gen.requirement_index_mapping, state, "[COMMAND_ACTIONS_WORKER]",
                full_requirements_text=state.inputs.requirements
            )
        except CATCHABLE_EXCEPTIONS as e:
            LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER] Failed to convert source references for aggregate '{aggregate_name}'", e)
            # 후처리 실패시에도 계속 진행하되, 에러 로그를 남김
        
        # 유효한 액션만 필터링
        actions = filter_valid_actions(current_gen.created_actions)
        
        # UUID 변환 처리
        actions = EsAliasTransManager(es_value).trans_to_uuid_in_actions(actions)
        
        # 액션 복원 작업 (boundedContextId 추가 등)
        actions = restore_actions(actions, es_value, current_gen.target_bounded_context_name)
        
        # 기존 요소와 중복되는 액션 필터링
        actions = filter_actions(actions, es_value)
        
        # 처리된 액션 저장
        current_gen.created_actions = actions

        # 워커에서는 ES 업데이트를 하지 않고 완료 표시만 함
        current_gen.generation_complete = True
        
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER] Postprocessing failed for aggregate '{aggregate_name}' in context '{bc_name}'", e)
        current_gen.retry_count += 1
        current_gen.created_actions = []
    
    return state

def worker_validate_command_actions(state: State) -> State:
    """
    Command 액션 생성 결과 검증 및 완료 처리 (워커 전용)
    - 생성 결과 검증
    - 완료 처리 또는 재시도 결정
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] No current generation found in worker validate")
        return state
        
    aggregate_name = current_gen.target_aggregate_name
    bc_name = current_gen.target_bounded_context_name

    try:
        if current_gen.retry_count > state.subgraphs.createCommandActionsByFunctionModel.max_retry_count:
            LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER] Maximum retry count exceeded for aggregate '{aggregate_name}' (retries: {current_gen.retry_count})")
            current_gen.is_failed = True
        
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER] Validation failed for aggregate '{aggregate_name}' in context '{bc_name}'", e)
        current_gen.is_failed = True

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[COMMAND_ACTIONS_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count > state.subgraphs.createCommandActionsByFunctionModel.max_retry_count:
            return "complete"

        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 전치리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.summarized_es_value:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.created_actions:
            return "generate"
        
        # 생성된 액션이 있으면 후처리 단계로 이동
        return "postprocess"
    
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_command_actions_worker_subgraph():
    """
    단일 Aggregate Command Actions 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_command_actions)
    worker_graph.add_node("generate", worker_generate_command_actions) 
    worker_graph.add_node("postprocess", worker_postprocess_command_actions)
    worker_graph.add_node("validate", worker_validate_command_actions)
    worker_graph.add_node("complete", lambda state: state)  # 완료 노드 (상태 그대로 반환)
    
    # 시작점을 전처리로 설정
    worker_graph.add_edge(START, "preprocess")
    
    # 조건부 엣지 추가
    worker_graph.add_conditional_edges(
        "preprocess",
        worker_decide_next_step,
        {
            "preprocess": "preprocess",
            "generate": "generate",
            "complete": "complete"
        }
    )
    
    worker_graph.add_conditional_edges(
        "generate",
        worker_decide_next_step,
        {
            "generate": "generate",
            "postprocess": "postprocess",
            "complete": "complete"
        }
    )
    
    worker_graph.add_conditional_edges(
        "postprocess",
        worker_decide_next_step,
        {
            "postprocess": "postprocess",
            "validate": "validate",
            "complete": "complete"
        }
    )
    
    worker_graph.add_conditional_edges(
        "validate",
        worker_decide_next_step,
        {
            "preprocess": "preprocess", 
            "generate": "generate",
            "postprocess": "postprocess", 
            "complete": "complete"
        }
    )
    
    # 컴파일
    compiled_worker = worker_graph.compile()
    
    def run_worker(state: State) -> State:
        """
        워커 서브그래프 실행 함수
        
        Args:
            state: current_generation에 처리할 Aggregate가 설정된 State
            
        Returns:
            State: 처리 완료된 current_generation을 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except CATCHABLE_EXCEPTIONS as e:
            LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker

# 유틸리티 함수들 (기존 subgraph에서 복사)
def filter_valid_actions(actions: List[ActionModel]) -> List[ActionModel]:
    """유효한 액션만 필터링"""
    # 기본 필터링 로직
    return [action for action in actions if 
            action.actionName and action.objectType and action.ids and action.ids.get("aggregateId")]

def restore_actions(actions: List[ActionModel], es_value, target_bounded_context_name: str) -> List[ActionModel]:
    """액션 복원 처리"""
    # 타겟 BoundedContext 찾기
    target_bounded_context = None
    for element in es_value.get("elements", {}).values():
        if (element and element.get("_type") == ELEMENT_TYPES.BOUNDED_CONTEXT and 
            element.get("name", "").lower() == target_bounded_context_name.lower()):
            target_bounded_context = element
            break
    
    if not target_bounded_context:
        raise ValueError(f"{target_bounded_context_name}에 대한 정보를 찾을 수 없습니다.")
    
    # 각 액션 복원
    for action in actions:
        if action.objectType == "Command":
            action.ids["boundedContextId"] = target_bounded_context.get("id")
            if not action.args:
                action.args = {}
            action.args["isRestRepository"] = False
        
        elif action.objectType == "Event":
            action.ids["boundedContextId"] = target_bounded_context.get("id")
            if not action.args:
                action.args = {}
        
        elif action.objectType == "ReadModel":
            action.ids["boundedContextId"] = target_bounded_context.get("id")
            if not action.args:
                action.args = {}
            action.args["properties"] = action.args.get("queryParameters", [])
    
    return actions

def get_id_by_name(name: str, actions: List[ActionModel], es_value) -> str:
    """이름으로 ID 찾기"""
    # 액션에서 찾기
    for action in actions:
        if (action.objectType == "Command" and action.args and 
            action.args.get("commandName") == name):
            return action.ids.get("commandId")
        if (action.objectType == "Event" and action.args and 
            action.args.get("eventName") == name):
            return action.ids.get("eventId")
    
    # ES Value에서 찾기
    for element in es_value.get("elements", {}).values():
        if element and element.get("name") == name and element.get("id"):
            return element.get("id")
    
    return None

def filter_actions(actions: List[ActionModel], es_value) -> List[ActionModel]:
    """중복 액션 필터링"""
    # 기존 요소 이름 목록
    es_names = [element.get("name") for element in es_value.get("elements", {}).values()
                if element and element.get("name")]
    
    display_names = [element.get("displayName").replace(" ", "") for element in es_value.get("elements", {}).values()
                    if element and element.get("displayName")]
    
    # 중복 및 검색/필터 관련 액션 제외
    filtered_actions = []
    for action in actions:
        if action.objectType == "Command":
            if (action.args.get("commandName") not in es_names and
                action.args.get("commandAlias").replace(" ", "") not in display_names and
                "search" not in action.args.get("commandName").lower() and
                "filter" not in action.args.get("commandName").lower()):
                filtered_actions.append(action)
        
        elif action.objectType == "Event":
            if (action.args.get("eventName") not in es_names and
                action.args.get("eventAlias").replace(" ", "") not in display_names and
                "search" not in action.args.get("eventName").lower() and
                "filter" not in action.args.get("eventName").lower()):
                filtered_actions.append(action)
        
        elif action.objectType == "ReadModel":
            if (action.args.get("readModelName") not in es_names and
                action.args.get("readModelAlias").replace(" ", "") not in display_names):
                filtered_actions.append(action)
    
    # 유효하지 않은 커맨드 액션 제거
    # 1. 모든 유효한 이벤트 ID 수집 (기존 es_value + 새로 생성된 action)
    all_event_ids = {
        element['id']
        for element in es_value.get("elements", {}).values()
        if element and element.get("_type") == ELEMENT_TYPES.EVENT and element.get('id')
    }
    all_event_ids.update({
        action.ids['eventId']
        for action in filtered_actions
        if action.objectType == "Event" and action.ids and action.ids.get('eventId')
    })

    # 2. Command의 outputEventIds를 검증하고, 유효한 이벤트가 없는 커맨드는 제거
    valid_commands_and_other_actions = []
    for action in filtered_actions:
        if action.objectType == "Command":
            if action.args and "outputEventIds" in action.args:
                valid_ids = [eid for eid in action.args.get("outputEventIds", []) if eid in all_event_ids]
                if valid_ids:
                    action.args["outputEventIds"] = valid_ids
                    valid_commands_and_other_actions.append(action)
                # else: 유효한 outputEventId가 하나도 없으면 커맨드 액션을 버립니다.
            else:
                # outputEventIds가 없는 커맨드는 그대로 유지합니다.
                valid_commands_and_other_actions.append(action)
        else:
            # Command가 아닌 다른 액션들은 그대로 유지합니다.
            valid_commands_and_other_actions.append(action)

    # 호출되지 않는 이벤트 제외
    output_event_ids = set()
    for action in valid_commands_and_other_actions:
        if action.objectType == "Command" and action.args and action.args.get("outputEventIds"):
            output_event_ids.update(action.args["outputEventIds"])
    
    return [
        action for action in valid_commands_and_other_actions
        if action.objectType != "Event" or (action.ids and action.ids.get("eventId") in output_event_ids)
    ]

def validate_required_events(required_event_names: List[str], action_models: List[ActionModel]) -> List[str]:
    """
    필수 이벤트가 생성된 액션에 포함되어 있는지 검증
    
    Args:
        required_event_names: 필수로 생성되어야 하는 이벤트 이름 목록
        action_models: 생성된 액션 모델 목록
    
    Returns:
        누락된 이벤트 이름 목록
    """
    # 생성된 이벤트 이름 목록 추출
    generated_event_names = set()
    for action in action_models:
        if action.objectType == "Event" and action.args and action.args["eventName"]:
            generated_event_names.add(action.args["eventName"])
    
    # 누락된 이벤트 찾기
    missing_events = [event_name for event_name in required_event_names 
                     if event_name not in generated_event_names]
    
    return missing_events

def validate_required_commands(required_command_names: List[str], action_models: List[ActionModel]) -> List[str]:
    """
    필수 커맨드가 생성된 액션에 포함되어 있는지 검증
    """
    # 생성된 커맨드 이름 목록 추출
    generated_command_names = set()
    for action in action_models:
        if action.objectType == "Command" and action.args and action.args["commandName"]:
            generated_command_names.add(action.args["commandName"])
    
    # 누락된 커맨드 찾기
    missing_commands = [command_name for command_name in required_command_names 
                       if command_name not in generated_command_names]
    
    return missing_commands

def validate_required_read_models(required_read_model_names: List[str], action_models: List[ActionModel]) -> List[str]:
    """
    필수 리드모델이 생성된 액션에 포함되어 있는지 검증
    """
    # 생성된 리드모델 이름 목록 추출
    generated_read_model_names = set()
    for action in action_models:
        if action.objectType == "ReadModel" and action.args and action.args["readModelName"]:
            generated_read_model_names.add(action.args["readModelName"])
    
    # 누락된 리드모델 찾기
    missing_read_models = [read_model_name for read_model_name in required_read_model_names 
                          if read_model_name not in generated_read_model_names]
    
    return missing_read_models
