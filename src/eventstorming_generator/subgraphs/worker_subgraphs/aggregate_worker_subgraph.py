"""
단일 Aggregate 처리를 위한 워커 서브그래프

이 모듈은 하나의 Aggregate에 대해 preprocess -> generate -> postprocess -> assign_missing_fields -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional, List, Dict, Any
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import AggregateGenerationState, ActionModel, State, CreateAggregateActionsByFunctionOutput, AssignFieldsToActionsGeneratorOutput
from ...utils import JsonUtil, LogUtil, CaseConvertUtil, EsAliasTransManager, EsTraceUtil, CreateAggregateByFunctionsUtil
from ...generators import CreateAggregateActionsByFunction, AssignFieldsToActionsGenerator
from ...constants import ELEMENT_TYPES
from ...config import Config
from eventstorming_generator.utils.catchable_exceptions import CATCHABLE_EXCEPTIONS

# 스레드로부터 안전한 컨텍스트 변수 생성
aggregate_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[AggregateGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createAggregateByFunctionsModel
    worker_id = aggregate_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[AGGREGATE_WORKER] Current worker ID not found in context")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[AGGREGATE_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_aggregate(state: State) -> State:
    """
    단일 Aggregate 전처리 (워커 전용)
    - 요약된 ES 값 생성
    - ID 변환 처리
    - 기존 요소 제거 등
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[AGGREGATE_WORKER] No current generation found in worker preprocess")
        return state
        
    try:
        if current_gen.description:
            current_gen.description = EsTraceUtil.add_line_numbers_to_description(current_gen.description)

        current_gen.target_aggregate_structure = CreateAggregateByFunctionsUtil.remove_id_value_objects(
            current_gen.target_aggregate_structure
        )
        current_gen.is_preprocess_completed = True
        
    except CATCHABLE_EXCEPTIONS as e:
        aggregate_name = current_gen.target_aggregate_structure.aggregateName
        LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Preprocessing failed for aggregate '{aggregate_name}'", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_aggregate(state: State) -> State:
    """
    Aggregate 생성 실행 (워커 전용)
    - Generator를 통한 Aggregate 액션 생성
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[AGGREGATE_WORKER] No current generation found in worker generate")
        return state
        
    try:

        generator = CreateAggregateActionsByFunction(
            model_name=Config.get_ai_model(),
            client={
                "inputs": {
                    "targetBoundedContextName": current_gen.target_bounded_context_name,
                    "description": current_gen.description,
                    "targetAggregateStructure": current_gen.target_aggregate_structure.model_dump(),
                    "attributesToGenerate": current_gen.attributes_to_generate
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
        generator_result:CreateAggregateActionsByFunctionOutput = generator_output["result"]
        actions = generator_result.aggregateActions + generator_result.valueObjectActions + generator_result.enumerationActions
        
        actionModels = [ActionModel(**action.model_dump()) for action in actions]
        for action in actionModels:
            action.type = "create"
            if action.objectType == "Aggregate":
                if generator_output["thinking"]:
                    action.args["description"] = "* Inference(When generating the aggregate)\n" + generator_output["thinking"] + "\n"

        current_gen.created_actions = actionModels
    
    except CATCHABLE_EXCEPTIONS as e:
        aggregate_name = current_gen.target_aggregate_structure.aggregateName
        LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Failed to generate aggregate '{aggregate_name}'", e)
        current_gen.retry_count += 1
    
    return state

def worker_postprocess_aggregate(state: State) -> State:
    """
    Aggregate 생성 후처리 작업 수행 (워커 전용)
    - 생성된 액션 검증
    - ID 변환
    - 워커에서는 ES 모델 업데이트 없이 액션만 저장
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[AGGREGATE_WORKER] No current generation found in worker postprocess")
        return state
        
    aggregate_name = current_gen.target_aggregate_structure.aggregateName

    try:
        if not current_gen.created_actions:
            LogUtil.add_error_log(state, f"[AGGREGATE_WORKER] No actions generated for aggregate '{aggregate_name}', incrementing retry count")
            current_gen.retry_count += 1
            return state
        
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        es_alias_trans_manager = EsAliasTransManager(es_value)
        
        actions = _filter_valid_property_actions(current_gen.created_actions)
        if not current_gen.is_action_postprocess_completed:
            # 액션 필터링 및 ID 변환
            actions = es_alias_trans_manager.trans_to_uuid_in_actions(actions)
            
            # Bounded Context ID 복원
            _restore_actions(
                actions, 
                es_value, 
                current_gen.target_bounded_context_name
            )
            
            # Aggregate ID 필터링
            actions = _filter_valid_aggregate_id_actions(
                actions,
                current_gen.target_aggregate_structure.aggregateName
            )

        # DDL 필드 포함 여부 검증
        missing_fields = []
        if current_gen.attributes_to_generate:
            all_generated_fields = set()
            for action in actions:
                if action.get("args") and "properties" in action.get("args", {}):
                    for prop in action.get("args", {}).get("properties", []):
                        if prop.get("name"):
                            all_generated_fields.add(CaseConvertUtil.camel_case(prop.get("name")))
            
            extracted_fields_set = {f for f in current_gen.attributes_to_generate}
            missing_fields = list(extracted_fields_set - all_generated_fields)
            
            if missing_fields:
                current_gen.missing_attributes = missing_fields
                current_gen.created_actions = [ActionModel(**action) for action in actions] # 필터링된 액션 저장
                current_gen.is_action_postprocess_completed = True
                LogUtil.add_warning_log(state, f"[AGGREGATE_WORKER] DDL fields missing for '{aggregate_name}': {missing_fields}. Routing to fix.")
                return state # 다음 단계(assign_missing_fields)로 보냄

        # Refs 후처리
        try:
            EsTraceUtil.convert_refs_to_indexes(
                actions, current_gen.original_description, current_gen.requirement_index_mapping,
                state, "[AGGREGATE_WORKER]",
                full_requirements_text=state.inputs.requirements
            )
        except CATCHABLE_EXCEPTIONS as e:
            LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Failed to convert source references for '{aggregate_name}'", e)
            # 후처리 실패시에도 계속 진행하되, 에러 로그를 남김
        
        # 워커에서는 ES 업데이트를 하지 않고 완료 표시만 함
        current_gen.created_actions = [ActionModel(**action) for action in actions]
        current_gen.generation_complete = True
        
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Postprocessing failed for aggregate '{aggregate_name}'", e)
        current_gen.retry_count += 1
        current_gen.created_actions = []
    
    return state

def worker_assign_missing_fields(state: State) -> State:
    """
    누락된 DDL 필드를 기존 Aggregate 또는 ValueObject에 할당 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen or not current_gen.missing_attributes:
        return state

    aggregate_name = current_gen.target_aggregate_structure.aggregateName
    try:

        elementAliasToUUIDDic = {}
        elementUUIDToAliasDic = {}
        existing_actions = []
        for action in current_gen.created_actions:
            if action.objectType == "Aggregate":
                aggregate_alias = "agg-" + action.args["aggregateName"]
                elementAliasToUUIDDic[aggregate_alias] = action.ids["aggregateId"]
                elementUUIDToAliasDic[action.ids["aggregateId"]] = aggregate_alias

                dumped_action = action.model_dump()
                dumped_action["ids"]["aggregateId"] = aggregate_alias
                if "boundedContextId" in dumped_action["ids"]:
                    del dumped_action["ids"]["boundedContextId"]
                if "refs" in dumped_action["args"]:
                    del dumped_action["args"]["refs"]
                if "properties" in dumped_action["args"]:
                    for prop in dumped_action["args"]["properties"]:
                        if "refs" in prop:
                            del prop["refs"]
                if "description" in dumped_action["args"]:
                    del dumped_action["args"]["description"]
                existing_actions.append(dumped_action)

        for action in current_gen.created_actions:
            if action.objectType == "ValueObject":
                value_object_alias = "vo-" + action.args["valueObjectName"]
                elementAliasToUUIDDic[value_object_alias] = action.ids["valueObjectId"]
                elementUUIDToAliasDic[action.ids["valueObjectId"]] = value_object_alias

                dumped_action = action.model_dump()
                dumped_action["ids"]["valueObjectId"] = value_object_alias
                if "boundedContextId" in dumped_action["ids"]:
                    del dumped_action["ids"]["boundedContextId"]
                if "refs" in dumped_action["args"]:
                    del dumped_action["args"]["refs"]
                if "properties" in dumped_action["args"]:
                    for prop in dumped_action["args"]["properties"]:
                        if "refs" in prop:
                            del prop["refs"]

                if elementUUIDToAliasDic[action.ids["aggregateId"]]:
                    dumped_action["ids"]["aggregateId"] = elementUUIDToAliasDic[action.ids["aggregateId"]]

                existing_actions.append(dumped_action)

        generator_inputs = {
            "description": current_gen.description,
            "existingActions": existing_actions,
            "missingFields": current_gen.missing_attributes
        }

        generator = AssignFieldsToActionsGenerator(
            model_name=Config.get_ai_model(),
            client={
                "inputs": generator_inputs,
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
        generator_result:AssignFieldsToActionsGeneratorOutput = generator_output["result"]

        assignments = generator_result.assignments
        invalid_properties = set(generator_result.invalid_properties)

        # Remove invalid properties from the list of fields to be checked against in the future.
        # This prevents the postprocess <-> assign_missing_fields loop.
        if invalid_properties:
            current_gen.attributes_to_generate = [
                field for field in current_gen.attributes_to_generate
                if field not in invalid_properties
            ]

        actions_map = {action.ids["aggregateId"] if action.objectType == "Aggregate" else action.ids["valueObjectId"]: action for action in current_gen.created_actions if action.objectType in ["Aggregate", "ValueObject"]}

        assigned_fields = set()
        if assignments:
            for assignment in assignments:
                parent_id_alias = assignment.parent_id
                if not parent_id_alias or parent_id_alias not in elementAliasToUUIDDic:
                    LogUtil.add_warning_log(state, f"[AGGREGATE_WORKER] Could not find parent with alias '{parent_id_alias}' to assign fields.")
                    continue

                parent_id = elementAliasToUUIDDic[parent_id_alias]
                if parent_id in actions_map:
                    parent_action = actions_map[parent_id]
                    props_to_add = assignment.properties_to_add
                    for prop_data in props_to_add:
                        assigned_fields.add(CaseConvertUtil.camel_case(prop_data.name))
                        if not any(p.get("name") == prop_data.name for p in parent_action.args["properties"]):
                            parent_action.args["properties"].append(prop_data.model_dump())
 
                else:
                    LogUtil.add_warning_log(state, f"[AGGREGATE_WORKER] Could not find parent with ID '{parent_id}' to assign fields.")

        original_missing_fields = set(current_gen.missing_attributes)
        # 할당된 필드와 유효하지 않은 필드를 모두 제외
        remaining_fields = original_missing_fields - assigned_fields - invalid_properties
        
        if not remaining_fields:
            current_gen.missing_attributes = []
            current_gen.retry_count = 0 # Reset retry count after successful assignment
        else:
            LogUtil.add_warning_log(state, f"[AGGREGATE_WORKER] Failed to assign all missing fields. Remaining: {list(remaining_fields)}. Retrying.")
            current_gen.missing_attributes = list(remaining_fields)
            current_gen.retry_count += 1
    
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Failed during field assignment for '{aggregate_name}'", e)
        current_gen.retry_count += 1
    
    return state

def worker_validate_aggregate(state: State) -> State:
    """
    Aggregate 생성 결과 검증 (워커 전용)
    - 생성 결과 검증
    - 완료 처리 또는 재시도 결정
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[AGGREGATE_WORKER] No current generation found in worker validate")
        return state
        
    aggregate_name = current_gen.target_aggregate_structure.aggregateName

    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count > state.subgraphs.createAggregateByFunctionsModel.max_retry_count:
            LogUtil.add_error_log(state, f"[AGGREGATE_WORKER] Maximum retry count exceeded for aggregate '{aggregate_name}' (retries: {current_gen.retry_count})")
            current_gen.is_failed = True

    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER] Validation failed for aggregate '{aggregate_name}'", e)
        current_gen.is_failed = True

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[AGGREGATE_WORKER] No current generation found in decide_next_step")
            return "complete"
        
        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count > state.subgraphs.createAggregateByFunctionsModel.max_retry_count:
            return "complete"

        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 전처리가 완료되지 않았으면 전처리 단계로 이동
        if not current_gen.is_preprocess_completed:
            return "preprocess"
        
        # 누락된 필드가 있으면 할당 단계로 이동
        if current_gen.missing_attributes:
            return "assign_missing_fields"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.created_actions:
            return "generate"
        
        # 생성된 액션이 있으면 후처리 단계로 이동
        return "postprocess"
    
    except CATCHABLE_EXCEPTIONS as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_aggregate_worker_subgraph():
    """
    단일 Aggregate 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_aggregate)
    worker_graph.add_node("generate", worker_generate_aggregate) 
    worker_graph.add_node("postprocess", worker_postprocess_aggregate)
    worker_graph.add_node("assign_missing_fields", worker_assign_missing_fields)
    worker_graph.add_node("validate", worker_validate_aggregate)
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
            "assign_missing_fields": "assign_missing_fields",
            "validate": "validate",
            "complete": "complete"
        }
    )

    worker_graph.add_conditional_edges(
        "assign_missing_fields",
        worker_decide_next_step,
        {
            "assign_missing_fields": "assign_missing_fields",
            "postprocess": "postprocess",
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
            "assign_missing_fields": "assign_missing_fields",
            "complete": "complete"
        }
    )
    
    # 컴파일
    compiled_worker = worker_graph.compile()
    
    def run_worker(state: State) -> State:
        """
        워커 서브그래프 실행 함수
        
        Args:
            state: worker_generations에 처리할 aggregate가 설정된 State
            
        Returns:
            State: 처리 완료된 aggregate를 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[AGGREGATE_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker

# 유틸리티 함수들
def _filter_valid_property_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    유효한 속성 액션 필터링
    """
    # 기본 검증
    actions = [
        action for action in actions
        if action.get("actionName") and action.get("objectType") and 
           action.get("ids") and action.get("ids", {}).get("aggregateId")
    ]
    
    # 객체 타입별 검증
    actions = [
        action for action in actions
        if (action.get("objectType") == "Aggregate") or
           (action.get("objectType") == "ValueObject" and 
            action.get("ids", {}).get("valueObjectId")) or
           (action.get("objectType") == "Enumeration" and 
            action.get("ids", {}).get("enumerationId"))
    ]
    
    # 속성 검증
    for action in actions:
        if action.get("args") and "properties" in action["args"]:
            action["args"]["properties"] = [
                prop for prop in action["args"]["properties"]
                if prop.get("name")
            ]
    
    return actions

def _filter_valid_aggregate_id_actions(actions: List[Dict[str, Any]], target_aggregate_name: str) -> List[Dict[str, Any]]:
    """
    유효한 Aggregate ID 액션 필터링
    """
    target_aggregate_name = target_aggregate_name.lower()
    
    # 유효한 Aggregate ID 목록 추출
    valid_aggregate_ids = [
        action.get("ids", {}).get("aggregateId")
        for action in actions
        if (action.get("objectType") == "Aggregate" and
            action.get("args") and
            action.get("args").get("aggregateName") and
            action.get("args").get("aggregateName").lower() == target_aggregate_name)
    ]
    
    # 유효한 Aggregate ID를 가진 액션만 필터링
    return [
        action for action in actions
        if action.get("ids", {}).get("aggregateId") in valid_aggregate_ids
    ]

def _restore_actions(actions: List[Dict[str, Any]], es_value: Dict[str, Any], target_bounded_context_name: str) -> None:
    """
    액션 데이터 복원
    """
    # 대상 Bounded Context 찾기
    target_bounded_context = _get_target_bounded_context(es_value, target_bounded_context_name)
    
    # Bounded Context ID 복원
    for action in actions:
        if action.get("objectType") in ["Aggregate", "Entity", "ValueObject", "Enumeration"]:
            action["ids"] = {
                "boundedContextId": target_bounded_context.get("id"),
                **action.get("ids", {})
            }

def _get_target_bounded_context(es_value: Dict[str, Any], target_bounded_context_name: str) -> Dict[str, Any]:
    """
    대상 Bounded Context 찾기
    """
    for element in es_value.get("elements", {}).values():
        if (element and 
            element.get("_type") == ELEMENT_TYPES.BOUNDED_CONTEXT and
            element.get("name", "").lower() == target_bounded_context_name.lower()):
            return element
    
    # 찾지 못한 경우 빈 객체 반환
    return {}
