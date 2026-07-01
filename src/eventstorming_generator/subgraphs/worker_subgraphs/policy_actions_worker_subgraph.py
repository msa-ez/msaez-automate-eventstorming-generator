"""
단일 Policy 액션 처리를 위한 워커 서브그래프

이 모듈은 하나의 Bounded Context에 대해 preprocess -> generate -> postprocess -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import PolicyActionGenerationState, State, ESValueSummaryGeneratorModel, CreatePolicyActionsByFunctionOutput
from ...utils import JsonUtil, LogUtil, ESValueSummarizeWithFilter, EsAliasTransManager, EsTraceUtil
from ...generators import CreatePolicyActionsByFunction
from ..es_value_summary_generator_sub_graph import create_es_value_summary_generator_subgraph
from ...config import Config

# 스레드로부터 안전한 컨텍스트 변수 생성
policy_actions_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[PolicyActionGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createPolicyActionsByFunctionModel
    worker_id = policy_actions_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[POLICY_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[POLICY_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_policy_actions(state: State) -> State:
    """
    단일 Policy 액션 전처리 (워커 전용)
    - state.subgraphs.createPolicyActionsByFunctionModel.worker_generations에서 해당 워커의 Policy를 가져와 처리
    - 요약된 ES 값 생성, 별칭 변환 관리자 생성
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[POLICY_WORKER] No current generation found in worker preprocess")
        return state
        
    try:
        # 기능 요구사항에 라인 번호 추가
        if current_gen.description:
            current_gen.description = EsTraceUtil.add_line_numbers_to_description(current_gen.description)

        # 현재 ES 값의 복사본 생성
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        
        # 요약된 ES 값 생성
        summarized_es_value = ESValueSummarizeWithFilter.get_summarized_es_value(
            es_value,
            ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["aggregateInnerStickers"] + 
            ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["detailedProperties"],
            EsAliasTransManager(es_value)
        )
        
        # 요약된 ES 값 저장
        current_gen.summarized_es_value = summarized_es_value
        
    except Exception as e:
        bc_name = current_gen.target_bounded_context_name
        LogUtil.add_exception_object_log(state, f"[POLICY_WORKER] Preprocessing failed for bounded context: '{bc_name}'", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_policy_actions(state: State) -> State:
    """
    단일 Policy 액션 생성 (워커 전용)
    - Generator를 통한 Policy 액션 생성
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[POLICY_WORKER] No current generation found in worker generate")
        return state
        
    bc_name = current_gen.target_bounded_context_name
    try:    
        # 모델명 가져오기
        model_name = Config.get_ai_model()
        
        # Generator 생성
        generator = CreatePolicyActionsByFunction(
            model_name=model_name,
            client={
                "inputs": {
                    "summarizedESValue": current_gen.summarized_es_value,
                    "description": current_gen.description
                },
                "preferredLanguage": state.inputs.preferedLanguage
            }
        )
        
        # 요약 서브그래프에서 처리 결과를 받아온 경우
        if (hasattr(state.subgraphs.esValueSummaryGeneratorModel, 'is_complete') and 
            state.subgraphs.esValueSummaryGeneratorModel.is_complete and 
            state.subgraphs.esValueSummaryGeneratorModel.processed_summarized_es_value):
            
            # 요약된 결과로 상태 업데이트
            current_gen.summarized_es_value = state.subgraphs.esValueSummaryGeneratorModel.processed_summarized_es_value
            
            # 요약 상태 초기화
            state.subgraphs.esValueSummaryGeneratorModel = ESValueSummaryGeneratorModel()
            
            current_gen.is_token_over_limit = False

            # 요약된 결과로 Generator 재생성
            generator = CreatePolicyActionsByFunction(
                model_name=model_name,
                client={
                    "inputs": {
                        "summarizedESValue": current_gen.summarized_es_value,
                        "description": current_gen.description
                    },
                    "preferredLanguage": state.inputs.preferedLanguage,
                    "retryCount": current_gen.retry_count
                }
            )

        # 토큰 초과 체크
        token_count = generator.get_token_count()
        model_max_input_limit = Config.get_ai_model_max_input_limit()
        
        if token_count > model_max_input_limit:  # 토큰 제한 초과시 요약 처리
            LogUtil.add_info_log(state, f"[POLICY_WORKER] Token limit exceeded for bounded context '{bc_name}' ({token_count} > {model_max_input_limit}), requesting summarization")
            
            left_generator = CreatePolicyActionsByFunction(
                model_name=model_name,
                client={
                    "inputs": {
                        "summarizedESValue": {},
                        "description": current_gen.description
                    },
                    "preferredLanguage": state.inputs.preferedLanguage,
                    "retryCount": current_gen.retry_count
                }
            )
            
            left_token_count = model_max_input_limit - left_generator.get_token_count()
            if left_token_count < 50:
                LogUtil.add_error_log(state, f"[POLICY_WORKER] Insufficient token space for bounded context '{bc_name}' summarization")
                current_gen.is_failed = True
                return state
            
            # ES 요약 생성 서브그래프 호출 준비
            # 요약 생성 모델 초기화
            state.subgraphs.esValueSummaryGeneratorModel = ESValueSummaryGeneratorModel(
                is_processing=False,
                is_complete=False,
                context=_build_worker_request_context(current_gen),
                keys_to_filter=ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["aggregateInnerStickers"] + 
                               ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["detailedProperties"],
                max_tokens=left_token_count,
                token_calc_model_vendor=Config.get_ai_model_vendor(),
                token_calc_model_name=Config.get_ai_model_name(),
                is_xml_format=True
            )
            
            # 토큰 초과시 요약 서브그래프 호출하고 현재 상태 반환
            current_gen.is_token_over_limit = True
            LogUtil.add_info_log(state, f"[POLICY_WORKER] Prepared ES value summary request for bounded context '{bc_name}' (available tokens: {left_token_count})")
            return state

        generator_output = generator.generate(
            bypass_cache=(current_gen.retry_count > 0),
            retry_count=current_gen.retry_count,
            extra_config_metadata={
                "job_id": state.inputs.jobId
            }
        )
        generator_result: CreatePolicyActionsByFunctionOutput = generator_output["result"]

        current_gen.extractedPolicies = [policy.model_dump() for policy in generator_result.extractedPolicies]
        current_gen.generation_complete = True

    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[POLICY_WORKER] Failed to generate policy actions for bounded context: '{bc_name}'", e)
        if current_gen:
            current_gen.retry_count += 1
    
    return state

def worker_postprocess_policy_actions(state: State) -> State:
    """
    단일 Policy 액션 후처리 (워커 전용)
    - 워커에서는 ES 모델 업데이트 없이 액션만 저장하고 완료 표시
    - 실제 ES 업데이트는 메인 오케스트레이터에서 일괄 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[POLICY_WORKER] No current generation found in worker postprocess")
        return state
    
    current_gen.generation_complete = True
    return state

def worker_validate_policy_actions(state: State) -> State:
    """
    단일 Policy 액션 검증 (워커 전용)
    - 생성 완료 및 재시도 횟수 확인
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[POLICY_WORKER] No current generation found in worker validate")
        return state
        
    bc_name = current_gen.target_bounded_context_name
    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count > state.subgraphs.createPolicyActionsByFunctionModel.max_retry_count:
            LogUtil.add_error_log(state, f"[POLICY_WORKER] Maximum retry count exceeded for bounded context '{bc_name}' (retries: {current_gen.retry_count})")
            current_gen.is_failed = True
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[POLICY_WORKER] Validation failed for bounded context: '{bc_name}'", e)
        current_gen.is_failed = True

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[POLICY_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count > state.subgraphs.createPolicyActionsByFunctionModel.max_retry_count:
            return "complete"
        
        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 토큰 초과 상태 확인
        if current_gen.is_token_over_limit and hasattr(state.subgraphs.esValueSummaryGeneratorModel, 'is_complete'):
            if state.subgraphs.esValueSummaryGeneratorModel.is_complete:
                return "generate"
            else:
                return "es_value_summary_generator"
        
        # 전처리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.summarized_es_value:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.extractedPolicies:
            return "generate"
        
        # 생성된 액션이 있으면 후처리 단계로 이동
        return "postprocess"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[POLICY_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_policy_actions_worker_subgraph():
    """
    단일 Policy 액션 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_policy_actions)
    worker_graph.add_node("generate", worker_generate_policy_actions) 
    worker_graph.add_node("postprocess", worker_postprocess_policy_actions)
    worker_graph.add_node("validate", worker_validate_policy_actions)
    worker_graph.add_node("complete", lambda state: state)  # 완료 노드 (상태 그대로 반환)
    worker_graph.add_node("es_value_summary_generator", create_es_value_summary_generator_subgraph())
    
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
            "es_value_summary_generator": "es_value_summary_generator",
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
        "es_value_summary_generator",
        worker_decide_next_step,
        {
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
            state: current_generation에 처리할 Policy가 설정된 State
            
        Returns:
            State: 처리 완료된 current_generation을 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[POLICY_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker

def _build_worker_request_context(current_gen: PolicyActionGenerationState) -> str:
    """
    요약 요청 컨텍스트 빌드
    """
    bounded_context_name = current_gen.target_bounded_context_name
    return f"""<context>
    <task>Creating policies for {bounded_context_name} Bounded Context</task>
    <description>
{current_gen.original_description}
    </description>
    <focus_points>
        <point>Events that should trigger commands in different aggregates or bounded contexts</point>
        <point>Business rules that require automatic reactions to system events</point>
        <point>Integration points between different parts of the domain</point>
        <point>Process flows that need automation through policies</point>
    </focus_points>
</context>"""
