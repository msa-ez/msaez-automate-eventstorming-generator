"""
단일 요구사항 청크 처리를 위한 워커 서브그래프

이 모듈은 하나의 요구사항 청크에 대해 preprocess -> generate -> postprocess -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import ContextMappingGenerationState, RequirementMappingGeneratorOutput, State
from ...utils import JsonUtil, LogUtil
from ...generators import RequirementMappingGenerator
from ...config import Config

# 스레드로부터 안전한 컨텍스트 변수 생성
context_mapping_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[ContextMappingGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createContextMappingModel
    worker_id = context_mapping_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_context_mapping(state: State) -> State:
    """
    단일 요구사항 청크 전처리 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] No current generation found in worker preprocess")
        return state
    
    try:
        current_gen.is_preprocess_completed = True
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER] Preprocessing failed for worker index: {current_gen.worker_index}", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_context_mapping(state: State) -> State:
    """
    단일 요구사항 청크 Context Mapping 생성 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] No current generation found in worker generate")
        return state
        
    try:

        model_name = Config.get_ai_model()
        
        generator = RequirementMappingGenerator(
            model_name=model_name,
            client={
                "inputs": {
                    "line_numbered_requirements": current_gen.line_numbered_requirements,
                    "boundedContexts": current_gen.boundedContexts
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
        generator_result: RequirementMappingGeneratorOutput = generator_output["result"]

        valid_boundedcontext_names = [bc.name for bc in current_gen.boundedContexts]
        current_gen.created_context_mappings = [cm for cm in generator_result.contextMappings if cm.boundedContextName in valid_boundedcontext_names]
        current_gen.generation_complete = True
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER] Failed to generate context mapping for worker index: {current_gen.worker_index}", e)
        if current_gen:
            current_gen.retry_count += 1

    return state

def worker_postprocess_context_mapping(state: State) -> State:
    """
    단일 요구사항 청크 Bounded Context 후처리 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] No current generation found in worker postprocess")
        return state
        
    
    try:
        # 생성된 Context Mappings가 없으면 실패로 처리
        if not current_gen.created_context_mappings or len(current_gen.created_context_mappings) == 0:
            current_gen.retry_count += 1
            return state
        
        current_gen.generation_complete = True
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER] Postprocessing failed for worker index: {current_gen.worker_index}", e)
        if current_gen:
            current_gen.retry_count += 1
            current_gen.created_context_mappings = []

    return state

def worker_validate_context_mapping(state: State) -> State:
    """
    단일 요구사항 청크 Context Mapping 검증 (워커 전용)
    - 생성 완료 및 재시도 횟수 확인
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] No current generation found in worker validate")
        return state
        
    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count > state.subgraphs.createContextMappingModel.max_retry_count:
            LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_WORKER] Maximum retry count exceeded for worker index: {current_gen.worker_index} (retries: {current_gen.retry_count})")
            current_gen.is_failed = True
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER] Validation failed for worker index: {current_gen.worker_index}", e)
        current_gen.is_failed = True

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[CONTEXT_MAPPING_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count > state.subgraphs.createContextMappingModel.max_retry_count:
            return "complete"
        
        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 전처리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.is_preprocess_completed:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.created_context_mappings or len(current_gen.created_context_mappings) == 0:
            return "generate"
        
        # 생성된 Context Mappings가 있으면 후처리 단계로 이동
        return "postprocess"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_context_mapping_worker_subgraph():
    """
    단일 Context Mapping 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_context_mapping)
    worker_graph.add_node("generate", worker_generate_context_mapping) 
    worker_graph.add_node("postprocess", worker_postprocess_context_mapping)
    worker_graph.add_node("validate", worker_validate_context_mapping)
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
            state: current_generation에 처리할 요구사항 청크가 설정된 State
            
        Returns:
            State: 처리 완료된 contextMappings를 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker
