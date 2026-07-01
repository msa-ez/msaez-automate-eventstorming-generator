"""
단일 클래스 ID 생성 처리를 위한 워커 서브그래프

이 모듈은 하나의 클래스 ID 생성 작업에 대해 preprocess -> generate -> postprocess -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import State, ClassIdGenerationState, ActionModel, CreateAggregateClassIdByDraftsOutput
from ...utils import JsonUtil, LogUtil, ESValueSummarizeWithFilter, EsAliasTransManager, EsUtils, CaseConvertUtil
from ...generators import CreateAggregateClassIdByDrafts
from ...config import Config

# 스레드로부터 안전한 컨텍스트 변수 생성
class_id_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[ClassIdGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createAggregateClassIdByDraftsModel
    worker_id = class_id_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[CLASS_ID_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[CLASS_ID_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_class_id_generation(state: State) -> State:
    """
    클래스 ID 생성을 위한 전처리 작업 수행 (워커 전용)
    - 요약된 ES 값 생성
    - ID 변환 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CLASS_ID_WORKER] No current generation found in worker preprocess")
        return state
    
    try:
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
  
        summarized_es_value = ESValueSummarizeWithFilter.get_summarized_es_value(
            es_value,
            ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["aggregateOuterStickers"] + ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["aggregateInnerStickers"],
            EsAliasTransManager(es_value)
        )

        specific_summarized_es_value = {
            "deletedProperties": summarized_es_value.get("deletedProperties", []),
            "boundedContexts": []
        }
        for bounded_context_data in summarized_es_value.get("boundedContexts", []):
            specific_bounded_context_data = {
                "id": bounded_context_data["id"],
                "name": bounded_context_data["name"],
                "aggregates": []
            }
            for aggregate_data in bounded_context_data.get("aggregates", []):
                if aggregate_data["name"] in current_gen.related_aggregate_names:
                    specific_bounded_context_data["aggregates"].append(aggregate_data)
            specific_summarized_es_value["boundedContexts"].append(specific_bounded_context_data)
        current_gen.summarized_es_value = specific_summarized_es_value
            
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER] Preprocessing failed for references: {', '.join(current_gen.target_references) if current_gen else 'Unknown'}", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_class_id(state: State) -> State:
    """
    클래스 ID 생성 실행 (워커 전용)
    - Generator를 통한 클래스 ID 액션 생성
    - 토큰 초과 확인 및 요약 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CLASS_ID_WORKER] No current generation found in worker generate")
        return state

    try:
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        
        generator = CreateAggregateClassIdByDrafts(
            model_name=Config.get_ai_model_light(),
            client={
                "inputs": {
                    "summarizedESValue": current_gen.summarized_es_value,
                    "draftOption": current_gen.draft_option,
                    "targetReferences": current_gen.target_references
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
        generator_result: CreateAggregateClassIdByDraftsOutput = generator_output["result"]
        actions = [action.model_dump() for action in generator_result.actions]
        if len(actions) == 0:
            current_gen.generation_complete = True
            return state
        
        es_alias_trans_manager = EsAliasTransManager(es_value)
        filtered_actions = _filter_invalid_actions(actions, current_gen.target_references, es_value, es_alias_trans_manager)
        filtered_actions = _filter_bidirectional_actions(filtered_actions, es_value, es_alias_trans_manager)
        if len(filtered_actions) == 0:
            current_gen.generation_complete = True
            return state
        
        actionModels = [ActionModel(**action) for action in filtered_actions]
        for action in actionModels:
            action.type = "create"

        current_gen.created_actions = actionModels
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER] Failed to generate class ID for references: {', '.join(current_gen.target_references) if current_gen else 'Unknown'}", e)
        if current_gen:
            current_gen.retry_count += 1
    
    return state

def worker_postprocess_class_id_generation(state: State) -> State:
    """
    클래스 ID 생성 후처리 작업 수행 (워커 전용)
    - 생성된 액션 검증
    - ID 변환
    - 워커에서는 ES 값 업데이트 없이 액션만 저장하고 완료 표시
    - 실제 ES 업데이트는 메인 오케스트레이터에서 일괄 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CLASS_ID_WORKER] No current generation found in worker postprocess")
        return state
    
    try:
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        
        # 생성된 액션이 없으면 완료로 처리
        if not current_gen.created_actions:
            current_gen.generation_complete = True
            return state
        
        # 액션 처리 및 필터링
        actions = current_gen.created_actions

        # ID 변환 및 액션 수정
        es_alias_trans_manager = EsAliasTransManager(es_value)
        actions = es_alias_trans_manager.trans_to_uuid_in_actions(actions)
        actions = _modify_actions_for_reference_class_value_object(actions, es_value)
    
        # 처리된 액션 저장
        current_gen.created_actions = actions
        current_gen.generation_complete = True

    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER] Postprocessing failed for references: {', '.join(current_gen.target_references) if current_gen else 'Unknown'}", e)
        if current_gen:
            current_gen.retry_count += 1
            current_gen.created_actions = []
    
    return state

def worker_validate_class_id_generation(state: State) -> State:
    """
    클래스 ID 생성 결과 검증 (워커 전용)
    - 생성 결과 검증
    - 완료 처리 또는 재시도 결정
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CLASS_ID_WORKER] No current generation found in worker validate")
        return state
        
    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count >= state.subgraphs.createAggregateClassIdByDraftsModel.max_retry_count:
            LogUtil.add_error_log(state, f"[CLASS_ID_WORKER] Max retry count exceeded for class ID generation. References: {', '.join(current_gen.target_references)} (retries: {current_gen.retry_count})")
            current_gen.is_failed = True
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER] Validation failed for references: {', '.join(current_gen.target_references) if current_gen else 'Unknown'}", e)
        current_gen.is_failed = True
    
    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[CLASS_ID_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count >= state.subgraphs.createAggregateClassIdByDraftsModel.max_retry_count:
            # ClassID 생성인 경우에는 실패를 해도, 다음 진행에 영향이 없기 때문에 그대로 진행
            current_gen.generation_complete = True
            return "complete"
        
        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 전처리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.summarized_es_value:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.created_actions:
            return "generate"
        
        # 생성된 액션이 있으면 후처리 단계로 이동
        return "postprocess"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_class_id_worker_subgraph():
    """
    단일 클래스 ID 생성 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_class_id_generation)
    worker_graph.add_node("generate", worker_generate_class_id) 
    worker_graph.add_node("postprocess", worker_postprocess_class_id_generation)
    worker_graph.add_node("validate", worker_validate_class_id_generation)
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
            "validate": "validate",
            "complete": "complete"
        }
    )
    
    worker_graph.add_conditional_edges(
        "postprocess",
        worker_decide_next_step,
        {
            "validate": "validate",
            "generate": "generate",
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
            state: worker_generations에 처리할 클래스 ID 작업이 설정된 State
            
        Returns:
            State: 처리 완료된 generation을 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[CLASS_ID_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker

# 유틸리티 함수들
def _filter_invalid_actions(actions, target_references, es_value, es_alias_trans_manager):
    """
    유효하지 않은 액션 필터링
    """
    filtered_actions = []
    seen_combinations = set()
    
    for action in actions:
        if not action.get("args") or not action["args"].get("valueObjectName"):
            continue
        
        if not action.get("ids") or not action["ids"].get("valueObjectId"):
            continue
        
        # 타겟 참조와 일치하는 액션만 유지
        is_valid_reference = any(
            target.lower() in action["args"]["valueObjectName"].lower()
            for target in target_references
        )
        
        if not is_valid_reference:
            continue

        # 동일한 boundedContextId + aggregateId + referenceClass 조합의 중복 액션 필터링
        # 인덱스상 뒤에 있는 중복 액션을 제외
        bounded_context_id = action["ids"].get("boundedContextId")
        aggregate_id = action["ids"].get("aggregateId")
        reference_class = action["args"].get("referenceClass")
        
        combination_key = f"{bounded_context_id}|{aggregate_id}|{reference_class}"
        if combination_key in seen_combinations:
            # 이미 동일한 조합의 액션이 있으므로 현재 액션은 중복으로 제외
            continue
        
        seen_combinations.add(combination_key)

        # esValue에 이미 존재하는 참조인지 확인하여 중복 필터링
        is_duplicate = False
        aggregate_uuid = es_alias_trans_manager.get_uuid_safely(aggregate_id)
        aggregate_element = es_value["elements"].get(aggregate_uuid)
        
        if aggregate_element and reference_class:
            entities = aggregate_element.get("aggregateRoot", {}).get("entities", {}).get("elements", {})
            for entity in entities.values():
                if entity.get("_type") == "org.uengine.uml.model.vo.Class":
                    field_descriptors = entity.get("fieldDescriptors", [])
                    for field in field_descriptors:
                        if field.get("referenceClass") == reference_class:
                            is_duplicate = True
                            break
                if is_duplicate:
                    break
        
        # 참조하는 referenceClass 이름이 실제 Aggregate의 name으로 존재하는지 확인
        is_reference_class_exists = False
        for element in es_value["elements"].values():
            if element and element.get("_type") == "org.uengine.modeling.model.Aggregate" and element.get("name") == reference_class:
                is_reference_class_exists = True
                break
        
        if not is_reference_class_exists:
            continue
        
        if not is_duplicate:
            filtered_actions.append(action)
    
    return filtered_actions

def _filter_bidirectional_actions(actions, es_value, es_alias_trans_manager):
    """
    양방향 참조 액션 필터링 (한 방향만 유지)
    """
    for i in range(len(actions)):
        action1 = actions[i]
        if not action1:
            continue
        
        agg1_id = action1["ids"]["aggregateId"]
        agg1_element = es_value["elements"].get(es_alias_trans_manager.get_uuid_safely(agg1_id))
        if not agg1_element:
            continue
        
        agg1_name = agg1_element.get("name", "")
        
        for j in range(i + 1, len(actions)):
            action2 = actions[j]
            if not action2:
                continue
            
            agg2_id = action2["ids"]["aggregateId"]
            agg2_element = es_value["elements"].get(es_alias_trans_manager.get_uuid_safely(agg2_id))
            if not agg2_element:
                continue
            
            agg2_name = agg2_element.get("name", "")
            
            # 양방향 참조 확인
            if (action1["args"]["referenceClass"] == agg2_name and 
                action2["args"]["referenceClass"] == agg1_name):
                # 두 번째 액션 제거 (첫 번째 방향만 유지)
                actions[j] = None
    
    return [action for action in actions if action]

def _modify_actions_for_reference_class_value_object(actions, es_value):
    """
    참조 클래스에 대한 액션 수정
    """
    actions_to_add = []
    
    for action in actions:
        if not action.args or not action.args["properties"]:
            continue
        
        # 참조할 Aggregate 찾기
        from_aggregate = _get_aggregate_by_id(es_value, action.ids["aggregateId"])
        to_aggregate = _get_aggregate_by_name(es_value, action.args["referenceClass"])
        
        if not from_aggregate or not to_aggregate:
            continue
        
        # 이름 형식 변경
        action.args["valueObjectName"] = f"{to_aggregate['name']}Id"
        
        # 키 속성 설정
        to_aggregate_key_prop = None
        if to_aggregate.get("aggregateRoot") and to_aggregate["aggregateRoot"].get("fieldDescriptors"):
            to_aggregate_key_prop = next(
                (prop for prop in to_aggregate["aggregateRoot"]["fieldDescriptors"] if prop.get("isKey")),
                None
            )
        
        if to_aggregate_key_prop:
            action_key_prop = next((prop for prop in action.args["properties"] if prop.get("isKey")), None)
            
            if action_key_prop:
                action_key_prop["name"] = to_aggregate_key_prop["name"]
                action_key_prop["type"] = to_aggregate_key_prop.get("className")
                action_key_prop["isKey"] = True
                action_key_prop["referenceClass"] = to_aggregate["name"]
                action_key_prop["isOverrideField"] = True
        
        # 추가 액션 생성 (Aggregate 업데이트)
        related_relation = _get_aggregate_relation(from_aggregate, to_aggregate, es_value)
        actions_to_add.append(
            ActionModel(
                objectType="Aggregate",
                type="update",
                ids={
                    "boundedContextId": action.ids["boundedContextId"],
                    "aggregateId": action.ids["aggregateId"]
                },
                args={
                    "fromValueObjectId": action.ids["valueObjectId"],
                    "relatedRelation": related_relation,
                    "properties": [
                        {
                            "name": CaseConvertUtil.camel_case(action.args["valueObjectName"]),
                            "type": action.args["valueObjectName"],
                            "referenceClass": to_aggregate["name"],
                            "isOverrideField": True
                        }
                    ]
                }
            )
        )
    
    actions.extend(actions_to_add)
    return actions

def _get_aggregate_by_id(es_value, aggregate_id):
    """ID로 Aggregate 찾기"""
    aggregate = es_value["elements"].get(aggregate_id)
    if aggregate and aggregate.get("_type") == "org.uengine.modeling.model.Aggregate":
        return aggregate
    return None

def _get_aggregate_by_name(es_value, aggregate_name):
    """이름으로 Aggregate 찾기"""
    for element in es_value["elements"].values():
        if (element and 
            element.get("_type") == "org.uengine.modeling.model.Aggregate" and 
            element.get("name") == aggregate_name):
            return element
    return None

def _get_aggregate_relation(from_aggregate, to_aggregate, es_value):
    """Aggregate 간 관계 추가"""
    # 이미 관계가 있는지 확인
    for relation in es_value["relations"].values():
        if (relation and relation.get("sourceElement") and relation.get("targetElement") and
            relation["sourceElement"].get("id") == from_aggregate.get("id") and
            relation["targetElement"].get("id") == to_aggregate.get("id")):
            return
    
    return EsUtils.getEventStormingRelationObjectBase(from_aggregate, to_aggregate)
