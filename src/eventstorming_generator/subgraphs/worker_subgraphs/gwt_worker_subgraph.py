"""
단일 GWT(Given-When-Then) 생성 처리를 위한 워커 서브그래프

이 모듈은 하나의 Command에 대해 preprocess -> generate -> postprocess -> validate
순으로 GWT 생성 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

import json
from typing import Optional
from contextvars import ContextVar
from copy import deepcopy
from langgraph.graph import StateGraph, START

from ...models import GWTGenerationState, State, ESValueSummaryGeneratorModel, CreateGWTGeneratorByFunctionOutput
from ...utils import JsonUtil, ESValueSummarizeWithFilter, EsAliasTransManager, LogUtil
from ...generators import CreateGWTGeneratorByFunction
from ..es_value_summary_generator_sub_graph import create_es_value_summary_generator_subgraph
from ...constants import ELEMENT_TYPES
from ...config import Config

# 스레드로부터 안전한 컨텍스트 변수 생성
gwt_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[GWTGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createGwtGeneratorByFunctionModel
    worker_id = gwt_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[GWT_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[GWT_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_gwt_generation(state: State) -> State:
    """
    단일 GWT 생성 전처리 (워커 전용)
    - ID 변환 매니저 생성
    - 요약된 ES 값 생성
    - 명령어 별칭 생성
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in worker preprocess")
        return state
        
    try:
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        
        es_alias_trans_manager = EsAliasTransManager(es_value)

        current_gen.target_command_alias = es_alias_trans_manager.uuid_to_alias_dic.get(
            current_gen.target_command_id, current_gen.target_command_id
        )

        target_bounded_context = None
        for element in es_value["elements"].values():
            if element.get("_type") == ELEMENT_TYPES.BOUNDED_CONTEXT and element.get("name") == current_gen.target_bounded_context_name:
                target_bounded_context = element
                break
        if not target_bounded_context:
            LogUtil.add_error_log(state, f"[GWT_WORKER] Target bounded context not found for command '{current_gen.target_command_id}' in aggregate '{current_gen.target_aggregate_name}'")
            current_gen.is_failed = True
            return state

        summarized_es_value = {
            "deletedProperties": [],
            "boundedContexts": [
                ESValueSummarizeWithFilter.get_summarized_bounded_context_value(
                    es_value,
                    target_bounded_context,
                    [],
                    es_alias_trans_manager
                )
            ]
        }
        
        current_gen.summarized_es_value = summarized_es_value
        
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        aggregate_name = current_gen.target_aggregate_name
        LogUtil.add_exception_object_log(state, f"[GWT_WORKER] Preprocessing failed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' context '{bc_name}'", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_gwt_generation(state: State) -> State:
    """
    단일 GWT 생성 실행 (워커 전용)
    - Generator를 통한 GWT 생성
    - 토큰 초과 확인 및 필요한 경우 요약 요청
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in worker generate")
        return state
    
    aggregate_name = current_gen.target_aggregate_name
    try:
        if current_gen.es_summary_complete and current_gen.is_token_over_limit:
            current_gen.is_token_over_limit = False
            current_gen.needs_es_summary = False
            current_gen.es_summary_processing = False
            LogUtil.add_info_log(state, f"[GWT_WORKER] ES summary processing completed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}'")
        

        model_name = Config.get_ai_model_light()

        generator = CreateGWTGeneratorByFunction(
            model_name=model_name,
            client={
                "inputs": {
                    "summarizedESValue": current_gen.summarized_es_value,
                    "description": current_gen.description,
                    "targetCommandAlias": current_gen.target_command_alias
                },
                "preferredLanguage": state.inputs.preferedLanguage,
                "retryCount": current_gen.retry_count
            }
        )
        
        token_count = generator.get_token_count()
        model_max_input_limit = Config.get_ai_model_light_max_input_limit()
        
        if token_count > model_max_input_limit:
            LogUtil.add_info_log(state, f"[GWT_WORKER] Token limit exceeded for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' ({token_count} > {model_max_input_limit}), requesting ES value summary")
            
            left_generator = CreateGWTGeneratorByFunction(
                model_name=model_name,
                client={
                    "inputs": {
                        "summarizedESValue": {},
                        "description": current_gen.description,
                        "targetCommandAlias": current_gen.target_command_alias
                    },
                    "preferredLanguage": state.inputs.preferedLanguage,
                    "retryCount": current_gen.retry_count
                }
            )

            left_token_count = model_max_input_limit - left_generator.get_token_count()
            if left_token_count < 50:
                LogUtil.add_error_log(state, f"[GWT_WORKER] Insufficient token space for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' (remaining: {left_token_count})")
                current_gen.is_failed = True
                return state

            # ES 요약 필요 상태로 설정
            current_gen.is_token_over_limit = True
            current_gen.needs_es_summary = True
            current_gen.es_summary_context = _build_worker_request_context(current_gen)
            current_gen.es_summary_max_tokens = left_token_count
            current_gen.es_summary_processing = False
            current_gen.es_summary_complete = False
            
            LogUtil.add_info_log(state, f"[GWT_WORKER] Token limit exceeded, will request ES summary for command '{current_gen.target_command_id}'")
            return state
        
        generator_output = generator.generate(
            bypass_cache=(current_gen.retry_count > 0),
            retry_count=current_gen.retry_count,
            extra_config_metadata={
                "job_id": state.inputs.jobId
            }
        )
        generator_result: CreateGWTGeneratorByFunctionOutput = generator_output["result"]
        
        processed_gwts = []
        for gwt in generator_result.gwts:
            # GWT json 로드시에 에러가 발생한 경우에는 해당 gwt에 대해서는 스킵 처리에서 작업 연속성을 우선시킴
            try:
                processed_gwt = {}
                processed_gwt["scenario"] = gwt.scenario
                processed_gwt["given"] = {
                    "aggregateName": gwt.given.aggregateName,
                    "aggregateValues": json.loads(gwt.given.aggregateValues)
                }
                processed_gwt["when"] = {
                    "commandName": gwt.when.commandName,
                    "commandValues": json.loads(gwt.when.commandValues)
                }
                processed_gwt["then"] = {
                    "eventName": gwt.then.eventName,
                    "eventValues": json.loads(gwt.then.eventValues)
                }
                processed_gwts.append(processed_gwt)
            except Exception as e:
                LogUtil.add_warning_log(state, f"[GWT_WORKER] Failed to process GWT for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}'. Skipping this GWT: {gwt.scenario}", e)
                continue

        command_to_replace = {}
        if processed_gwts and len(processed_gwts) > 0:
            es_value = {
                "elements": state.outputs.esValue.elements,
                "relations": state.outputs.esValue.relations
            }

            es_alias_trans_manager = EsAliasTransManager(es_value)
            target_command_id = es_alias_trans_manager.alias_to_uuid_dic.get(
                current_gen.target_command_alias, current_gen.target_command_id
            )
            
            if target_command_id and target_command_id in es_value["elements"]:
                target_command = deepcopy(es_value["elements"][target_command_id])
                
                target_command["description"] = _make_command_description(processed_gwts)
                examples = _get_examples(processed_gwts, es_value)
                if examples and len(examples) > 0:
                    target_command["examples"] = examples
                    command_to_replace = target_command
        
        current_gen.command_to_replace = command_to_replace
    
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        LogUtil.add_exception_object_log(state, f"[GWT_WORKER] Failed to generate GWT for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' context '{bc_name}'", e)
        current_gen.retry_count += 1

    return state

def worker_postprocess_gwt_generation(state: State) -> State:
    """
    단일 GWT 생성 후처리 (워커 전용)
    - 워커에서는 ES 모델 업데이트 없이 command_to_replace만 저장하고 완료 표시
    - 실제 ES 업데이트는 메인 오케스트레이터에서 일괄 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in worker postprocess")
        return state
        
    aggregate_name = current_gen.target_aggregate_name
    try:
        if not current_gen.command_to_replace:
            current_gen.is_failed = True
            return state
        
        current_gen.generation_complete = True
    
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        LogUtil.add_exception_object_log(state, f"[GWT_WORKER] Postprocessing failed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' context '{bc_name}'", e)
        current_gen.retry_count += 1
        current_gen.command_to_replace = {}

    return state

def worker_validate_gwt_generation(state: State) -> State:
    """
    단일 GWT 생성 검증 (워커 전용)
    - 생성 완료 및 재시도 횟수 확인
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in worker validate")
        return state
        
    aggregate_name = current_gen.target_aggregate_name
    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count > state.subgraphs.createGwtGeneratorByFunctionModel.max_retry_count:
            LogUtil.add_error_log(state, f"[GWT_WORKER] Maximum retry count exceeded for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' (retries: {current_gen.retry_count})")
            current_gen.generation_complete = False  # 실패로 표시하되 완료는 False로 유지
        
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        LogUtil.add_exception_object_log(state, f"[GWT_WORKER] Validation failed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' context '{bc_name}'", e)
        current_gen.generation_complete = False

    return state

def worker_process_es_summary(state: State) -> State:
    """
    워커에서 ES 요약을 처리하는 노드
    - 글로벌 esValueSummaryGeneratorModel을 사용하여 요약 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in worker ES summary")
        return state
    
    aggregate_name = current_gen.target_aggregate_name
    try:
        # ES 요약 처리가 시작되지 않았으면 초기화
        if current_gen.needs_es_summary and not current_gen.es_summary_processing:
            # ES 요약 생성 모델 초기화
            state.subgraphs.esValueSummaryGeneratorModel = ESValueSummaryGeneratorModel(
                is_processing=False,
                is_complete=False,
                context=current_gen.es_summary_context,
                keys_to_filter=[],  # GWT 생성에 필요한 필터 설정
                max_tokens=current_gen.es_summary_max_tokens,
                token_calc_model_vendor=Config.get_ai_model_light_vendor(),
                token_calc_model_name=Config.get_ai_model_light_name(),
                is_xml_format=True
            )
            
            current_gen.es_summary_processing = True
            LogUtil.add_info_log(state, f"[GWT_WORKER] Starting ES summary processing for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}'")
            return state
        
        # ES 요약 서브그래프 실행 후 결과 확인
        if (current_gen.es_summary_processing and 
            state.subgraphs.esValueSummaryGeneratorModel.is_complete and 
            state.subgraphs.esValueSummaryGeneratorModel.processed_summarized_es_value):
            
            # 요약된 결과로 상태 업데이트
            current_gen.summarized_es_value = state.subgraphs.esValueSummaryGeneratorModel.processed_summarized_es_value
            
            # 요약 상태 초기화
            state.subgraphs.esValueSummaryGeneratorModel = ESValueSummaryGeneratorModel()
            
            # 요약 완료 표시
            current_gen.es_summary_complete = True
            current_gen.es_summary_processing = False
            current_gen.needs_es_summary = False
            
            LogUtil.add_info_log(state, f"[GWT_WORKER] ES summary completed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}'")
    
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        LogUtil.add_exception_object_log(state, f"[GWT_WORKER] ES summary processing failed for command '{current_gen.target_command_id}' in aggregate '{aggregate_name}' context '{bc_name}'", e)
        current_gen.retry_count += 1

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[GWT_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 최대 재시도 횟수 초과 시 완료
        if current_gen.retry_count > state.subgraphs.createGwtGeneratorByFunctionModel.max_retry_count:
            return "complete"
        
        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # ES 요약이 필요한 경우 ES 요약 단계로 이동
        if current_gen.needs_es_summary and not current_gen.es_summary_processing:
            return "es_value_summary"
        
        # ES 요약이 완료된 경우 이를 처리하기 위한 단계로 이동
        if current_gen.es_summary_processing and state.subgraphs.esValueSummaryGeneratorModel.is_complete and state.subgraphs.esValueSummaryGeneratorModel.processed_summarized_es_value:
            return "es_value_summary"
        
        # ES 요약이 진행 중인 경우 ES 요약 서브그래프 호출
        if current_gen.es_summary_processing and not current_gen.es_summary_complete:
            return "es_value_summary_generator"
        
        # ES 요약이 완료된 경우 생성 단계로 이동
        if current_gen.es_summary_complete and current_gen.is_token_over_limit:
            return "generate"
        
        # 전처리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.summarized_es_value:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.command_to_replace:
            return "generate"
        
        # 생성된 GWT가 있으면 후처리 단계로 이동
        return "postprocess"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_gwt_worker_subgraph():
    """
    단일 GWT 생성 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_gwt_generation)
    worker_graph.add_node("generate", worker_generate_gwt_generation) 
    worker_graph.add_node("postprocess", worker_postprocess_gwt_generation)
    worker_graph.add_node("validate", worker_validate_gwt_generation)
    worker_graph.add_node("es_value_summary", worker_process_es_summary)
    worker_graph.add_node("es_value_summary_generator", create_es_value_summary_generator_subgraph())
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
            "es_value_summary": "es_value_summary",
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
    
    worker_graph.add_conditional_edges(
        "es_value_summary",
        worker_decide_next_step,
        {
            "es_value_summary": "es_value_summary",
            "es_value_summary_generator": "es_value_summary_generator",
            "generate": "generate",
            "complete": "complete"
        }
    )
    
    worker_graph.add_conditional_edges(
        "es_value_summary_generator",
        worker_decide_next_step,
        {
            "es_value_summary": "es_value_summary",
            "generate": "generate",
            "complete": "complete"
        }
    )
    
    # 컴파일
    compiled_worker = worker_graph.compile()
    
    def run_worker(state: State) -> State:
        """
        워커 서브그래프 실행 함수
        
        Args:
            state: worker_generations에 처리할 GWT가 설정된 State
            
        Returns:
            State: 처리 완료된 worker_generations를 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[GWT_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.generation_complete = False
                current_gen.retry_count += 1
            return state
    
    return run_worker

def _build_worker_request_context(current_gen: GWTGenerationState) -> str:
    """
    요약 요청 컨텍스트 빌드 (워커용) - XML 구조화된 프롬프트
    """
    target_bounded_context_name = current_gen.target_bounded_context_name
    target_aggregate_name = current_gen.target_aggregate_name
    target_command_alias = current_gen.target_command_alias
    description = current_gen.description
    
    return f"""<instruction>
    <core_instructions>
        <title>Task: Generate Given-When-Then (GWT) Test Scenarios</title>
        <task_description>Focus on generating comprehensive Given-When-Then (GWT) test scenarios for a specific command within the event storming domain model. The scenarios should validate the behavior and business rules of the target command.</task_description>
        
        <context_information>
            <title>Domain Context</title>
            <bounded_context>{target_bounded_context_name}</bounded_context>
            <target_command>{target_command_alias}</target_command>
            <target_aggregate>{target_aggregate_name}</target_aggregate>
        </context_information>
        
        <business_requirements>
            <title>Business Requirements</title>
            <description>{description}</description>
        </business_requirements>
        
        <prioritization_guidelines>
            <title>Element Prioritization Guidelines</title>
            <guideline id="direct_relation" priority="1">Directly related to the target command and its associated events</guideline>
            <guideline id="same_aggregate" priority="2">Part of the same aggregate as the target command</guideline>
            <guideline id="referenced_elements" priority="3">Referenced by the target command or its events</guideline>
            <guideline id="business_related" priority="4">Related to the business requirements provided</guideline>
            <guideline id="bounded_context" priority="5">Part of the specified bounded context</guideline>
        </prioritization_guidelines>
        
        <purpose>
            <title>Purpose Statement</title>
            <description>This context is specifically designed to generate comprehensive GWT scenarios that validate the behavior and business rules of the target command. The prioritization ensures that the most relevant domain elements are included to create meaningful test scenarios that accurately reflect the business domain and its constraints.</description>
        </purpose>
    </core_instructions>
</instruction>"""

def _make_command_description(gwts):
    """
    적절한 Command Description을 생성하는 함수
    """
    description = ""

    if gwts and len(gwts) > 0:
        description += "* Generated example scenarios\n"
        for i, gwt in enumerate(gwts):
            description += f"{i+1}. {gwt['scenario']}\n"
    
    return description

def _get_examples(gwts, es_value):
    """
    GWT 시나리오로부터 examples 데이터 구조를 생성하는 함수
    """
    examples = []
    for gwt in gwts:
        if not gwt.get("given") or not gwt.get("when") or not gwt.get("then"):
            continue
        
        given_element = _find_element_by_name(gwt["given"]["aggregateName"], es_value, "Aggregate")
        when_element = _find_element_by_name(gwt["when"]["commandName"], es_value, "Command")
        then_element = _find_element_by_name(gwt["then"]["eventName"], es_value, "Event")
        
        if not given_element or not when_element or not then_element:
            continue
            
        if not given_element.get("_type", "").endswith("Aggregate") or \
           not when_element.get("_type", "").endswith("Command") or \
           not then_element.get("_type", "").endswith("Event"):
            continue
        
        examples.append({
            "given": [{
                "type": "Aggregate",
                "name": gwt["given"]["aggregateName"],
                "value": _get_values_using_field_descriptors(gwt["given"]["aggregateValues"], given_element.get("aggregateRoot", {}).get("fieldDescriptors", []))
            }],
            "when": [{
                "type": "Command",
                "name": gwt["when"]["commandName"],
                "value": _get_values_using_field_descriptors(gwt["when"]["commandValues"], when_element.get("fieldDescriptors", []))
            }],
            "then": [{
                "type": "Event",
                "name": gwt["then"]["eventName"],
                "value": _get_values_using_field_descriptors(gwt["then"]["eventValues"], then_element.get("fieldDescriptors", []))
            }]
        })
    
    return examples

def _find_element_by_name(name: str, es_value, element_type: str):
    """
    이름으로 엘리먼트를 찾는 함수
    """
    for element in es_value.get("elements", {}).values():
        if element and element.get("name") == name and element_type in element.get("_type", ""):
            return element
    return {}

def _get_values_using_field_descriptors(values, field_descriptors):
    """
    필드 설명자를 사용하여 값을 추출하는 함수
    """
    return_values = {}
    for field_descriptor in field_descriptors:
        field_name = field_descriptor.get("name")
        if field_name:
            return_values[field_name] = values.get(field_name, "N/A")
    return return_values
