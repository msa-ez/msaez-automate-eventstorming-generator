import time
import uuid
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import GWTGenerationState, State
from ..utils import JsonUtil, LogUtil
from ..utils.job_utils import JobUtil
from .worker_subgraphs import create_gwt_worker_subgraph, gwt_worker_id_context
from ..constants import RESUME_NODES, ELEMENT_TYPES
from ..config import Config


def resume_from_create_gwt(state: State):
    try :

        state.subgraphs.createGwtGeneratorByFunctionModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_GWT and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_GWT.__dict__.values():
                LogUtil.add_info_log(state, f"[GWT_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createGwtGeneratorByFunctionModel.is_failed = True
                LogUtil.add_error_log(state, f"[GWT_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[GWT_SUBGRAPH] Starting GWT generation process (parallel mode)")
        return "prepare"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during resume_from_create_gwt", e)
        state.subgraphs.createGwtGeneratorByFunctionModel.is_failed = True
        return "complete"

def prepare_gwt_generation(state: State) -> State:
    """
    GWT 생성을 위한 준비 작업 수행
    - 초안 데이터 설정
    - 처리할 Command 목록 초기화
    """
    
    try:

        # 이미 처리 중이면 상태 유지
        if state.subgraphs.createGwtGeneratorByFunctionModel.is_processing:
            return state
        
        state.subgraphs.createGwtGeneratorByFunctionModel.is_processing = True
        state.subgraphs.createGwtGeneratorByFunctionModel.all_complete = False
        
        pending_generations = []
        total_aggregates = 0
        total_commands = 0
        worker_index = 0

        for structure in state.inputs.draft.structures:
            bc_name = structure.boundedContextName
            description = state.inputs.draft.metadatas.boundedContextRequirements.get(bc_name, "")

            target_aggregates = []
            for element in state.outputs.esValue.elements.values():
                if element and element.get("_type") == ELEMENT_TYPES.AGGREGATE:
                    bounded_context_id = element.get("boundedContext", {}).get("id")
                    bounded_context = state.outputs.esValue.elements[bounded_context_id]
                    if bounded_context and bounded_context.get("name") == bc_name:
                        target_aggregates.append(element)

            for target_aggregate in target_aggregates:
                target_command_ids = []
                target_aggregate_name = target_aggregate.get("name", "Unknown")
                
                for element in state.outputs.esValue.elements.values():
                    if element and element.get("_type") == ELEMENT_TYPES.COMMAND and \
                       element.get("aggregate", {}).get("id") == target_aggregate.get("id"):
                        target_command_ids.append(element.get("id"))
                
                if not target_command_ids:
                    continue
                
                for target_command_id in target_command_ids:
                    generation_state = GWTGenerationState(
                        target_bounded_context_name=bc_name,
                        target_command_id=target_command_id,
                        target_aggregate_name=target_aggregate_name,
                        description=description,
                        worker_index=worker_index,
                        retry_count=0,
                        generation_complete=False
                    )
                    pending_generations.append(generation_state)
                    total_commands += 1
                    worker_index += 1

                total_aggregates += 1
        state.subgraphs.createGwtGeneratorByFunctionModel.pending_generations = pending_generations
        
        LogUtil.add_info_log(state, f"[GWT_SUBGRAPH] Preparation completed. Total tasks: {len(pending_generations)} ({total_aggregates} aggregates, {total_commands} commands)")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during GWT generation preparation", e)
        state.subgraphs.createGwtGeneratorByFunctionModel.is_failed = True

    return state

def select_batch_gwt_generation(state: State) -> State:
    """
    다음 배치로 처리할 GWT들을 선택 (병렬 처리용)
    - batch_size만큼의 GWT를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_GWT
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_GWT.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createGwtGeneratorByFunctionModel
        batch_size = Config.get_ai_model_light_max_batch_size()

        # 모든 처리가 완료되었는지 확인
        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        # 현재 처리 중인 배치가 있으면 상태 유지
        if model.current_batch:
            return state
        
        # 대기 중인 GWT들에서 배치 크기만큼 선택
        if model.pending_generations:
            # 남은 GWT 수와 배치 크기 중 작은 값만큼 선택
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            
            model.current_batch = current_batch
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed to select GWT batch", e)
        state.subgraphs.createGwtGeneratorByFunctionModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 GWT들을 병렬로 처리
    - 각 GWT를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createGwtGeneratorByFunctionModel
    
    if not model.current_batch:
        return state
        
    batch_size = len(model.current_batch)

    try:
        # 워커 서브그래프 인스턴스 생성
        worker_function = create_gwt_worker_subgraph()
        
        # 각 GWT에 대해 워커 ID 생성 및 worker_generations에 저장
        worker_ids = []
        for gwt_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = gwt_generation_state
        
        def execute_single_worker(worker_id: str) -> GWTGenerationState:
            """
            단일 GWT를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                # 현재 스레드의 컨텍스트에 worker_id 설정
                gwt_worker_id_context.set(worker_id)

                gwt_generation_state = model.worker_generations[worker_id]
                command_alias = gwt_generation_state.target_command_alias or gwt_generation_state.target_command_id
                aggregate_name = gwt_generation_state.target_aggregate_name
                
                # 워커 실행
                result_state = worker_function(state)
                
                # 결과에서 처리된 GWT 상태 추출
                completed_gwt = result_state.subgraphs.createGwtGeneratorByFunctionModel.worker_generations.get(worker_id)
                
                if completed_gwt and completed_gwt.generation_complete:
                    return completed_gwt
                elif completed_gwt and completed_gwt.retry_count > model.max_retry_count:
                    LogUtil.add_error_log(state, f"[GWT_WORKER_EXECUTOR] Worker failed for GWT '{command_alias}' in aggregate '{aggregate_name}' (max retries exceeded)")
                    return completed_gwt
                else:
                    LogUtil.add_error_log(state, f"[GWT_WORKER_EXECUTOR] Worker returned incomplete result for GWT '{command_alias}' in aggregate '{aggregate_name}'")
                    gwt_generation_state.retry_count += 1
                    return gwt_generation_state
                    
            except Exception as e:
                gwt_generation_state = model.worker_generations.get(worker_id)
                if gwt_generation_state:
                    command_alias = gwt_generation_state.target_command_alias or gwt_generation_state.target_command_id
                    aggregate_name = gwt_generation_state.target_aggregate_name
                    LogUtil.add_exception_object_log(state, f"[GWT_WORKER_EXECUTOR] Worker execution failed for GWT '{command_alias}' in aggregate '{aggregate_name}'", e)
                    gwt_generation_state.retry_count += 1
                    return gwt_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[GWT_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)
                    # 빈 실패 상태 반환
                    failed_state = GWTGenerationState()
                    failed_state.retry_count = model.max_retry_count + 1
                    return failed_state
        
        # ThreadPoolExecutor를 사용한 병렬 실행
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # 모든 워커 제출
            future_to_worker_id = {
                executor.submit(execute_single_worker, worker_id): worker_id 
                for worker_id in worker_ids
            }
            
            # 결과 수집
            completed_results = []
            for future in as_completed(future_to_worker_id):
                worker_id = future_to_worker_id[future]
                original_gwt = model.worker_generations[worker_id]
                try:
                    result_gwt = future.result()
                    completed_results.append(result_gwt)
                    
                except Exception as e:
                    command_alias = original_gwt.target_command_alias or original_gwt.target_command_id
                    aggregate_name = original_gwt.target_aggregate_name
                    LogUtil.add_exception_object_log(state, f"[GWT_SUBGRAPH] Failed to get worker result for GWT '{command_alias}' in aggregate '{aggregate_name}'", e)
                    original_gwt.retry_count += 1
                    completed_results.append(original_gwt)
        
        # 결과를 parallel_worker_results에 저장
        model.parallel_worker_results = completed_results
        
        # 사용된 worker_generations 정리 (메모리 절약)
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if not result.generation_complete)
        
        LogUtil.add_info_log(state, f"[GWT_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True

    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 GWT들의 결과를 ES에 일괄 적용
    - 완료된 GWT들을 completed_generations로 이동
    """
    model = state.subgraphs.createGwtGeneratorByFunctionModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        # ES 값의 복사본 생성
        es_value = state.outputs.esValue.model_dump()
        
        successful_gwts = []
        failed_gwts = []
        
        # 모든 성공한 GWT들의 결과 수집하고 적용
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for gwt_result in model.parallel_worker_results:
            command_alias = gwt_result.target_command_alias or gwt_result.target_command_id
            aggregate_name = gwt_result.target_aggregate_name
            
            if gwt_result.generation_complete and gwt_result.command_to_replace:
                successful_gwts.append(gwt_result)
                
                command_id = gwt_result.command_to_replace.get("id")
                if command_id and command_id in es_value["elements"]:
                    es_value["elements"][command_id] = gwt_result.command_to_replace
                    
            else:
                failed_gwts.append(gwt_result)
                LogUtil.add_error_log(state, f"[GWT_SUBGRAPH] GWT generation failed or has no result for command '{command_alias}' in aggregate '{aggregate_name}'")
        
        if successful_gwts:
            state.outputs.esValue.elements = es_value["elements"]
            
        # 성공한 GWT들을 완료 목록으로 이동 (변수 정리)
        for gwt in successful_gwts:
            # 메모리 절약을 위한 변수 정리
            gwt.target_command_id = ""
            gwt.target_aggregate_name = ""
            gwt.description = ""
            gwt.summarized_es_value = {}
            gwt.target_command_alias = ""
            gwt.command_to_replace = {}
            
            model.completed_generations.append(gwt)
        
        # 실패한 GWT들도 완료 목록으로 이동 (재시도는 하지 않음)
        for gwt in failed_gwts:
            gwt.target_command_id = ""
            gwt.target_aggregate_name = ""
            gwt.description = ""
            gwt.summarized_es_value = {}
            gwt.target_command_alias = ""
            gwt.command_to_replace = {}
            
            model.completed_generations.append(gwt)
        
        # 배치 처리 완료 정리
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_gwts)
        failed_count = len(failed_gwts)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[GWT_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True

    return state

def complete_processing(state: State) -> State:
    """
    GWT 생성 프로세스 완료
    """
    model = state.subgraphs.createGwtGeneratorByFunctionModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[GWT_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_GWT
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_GWT.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[GWT_SUBGRAPH] GWT generation process completed with failures. Successfully processed: {completed_count} command tasks")
        else:
            LogUtil.add_info_log(state, f"[GWT_SUBGRAPH] GWT generation process completed successfully. Total processed: {completed_count} command tasks")
        
        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during GWT generation process completion", e)
        model.is_failed = True

    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createGwtGeneratorByFunctionModel

        if model.is_failed:
            return "complete"

        # 모든 작업이 완료되었으면 완료 상태로 이동
        if model.all_complete:
            return "complete"
        
        # 병렬 워커 결과가 있으면 결과 수집 및 적용 단계로 이동
        if model.parallel_worker_results:
            return "collect_results"
        
        # 현재 처리 중인 배치가 있으면 병렬 실행 단계로 이동
        if model.current_batch:
            return "execute_parallel"
        
        # 대기 중인 작업이 있으면 배치 선택 단계로 이동
        if model.pending_generations:
            return "select_batch"
            
        # 아무것도 없으면 완료
        return "complete"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[GWT_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createGwtGeneratorByFunctionModel.is_failed = True
        return "complete"

def create_gwt_generator_by_function_subgraph() -> Callable:
    """
    Command GWT 생성 서브그래프 생성 (병렬 처리 지원)
    """
    # 서브그래프 정의
    subgraph = StateGraph(State)
    
    # 새로운 병렬 처리 노드들 추가
    subgraph.add_node("prepare", prepare_gwt_generation)
    subgraph.add_node("select_batch", select_batch_gwt_generation)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)
    
    # 엣지 추가 (새로운 병렬 처리 플로우)
    subgraph.add_conditional_edges(START, resume_from_create_gwt, {
        "prepare": "prepare",
        "select_batch": "select_batch",
        "execute_parallel": "execute_parallel",
        "collect_results": "collect_results",
        "complete": "complete"
    })

    # 새로운 병렬 처리 플로우 엣지들
    subgraph.add_conditional_edges(
        "prepare",
        decide_next_step,
        {
            "select_batch": "select_batch",
            "complete": "complete"
        }
    )
    
    subgraph.add_conditional_edges(
        "select_batch",
        decide_next_step,
        {
            "select_batch": "select_batch",
            "execute_parallel": "execute_parallel", 
            "complete": "complete"
        }
    )
    
    subgraph.add_conditional_edges(
        "execute_parallel",
        decide_next_step,
        {
            "collect_results": "collect_results",
            "complete": "complete"
        }
    )
    
    subgraph.add_conditional_edges(
        "collect_results",
        decide_next_step,
        {
            "select_batch": "select_batch",
            "complete": "complete"
        }
    )
    
    # 컴파일된 그래프 반환
    compiled_subgraph = subgraph.compile()
    
    # 서브그래프 실행 함수
    def run_subgraph(state: State) -> State:
        """
        서브그래프 실행 함수
        """
        # 서브그래프 실행
        result = State(**compiled_subgraph.invoke(state, {"recursion_limit": 2147483647}))
        return result
    
    return run_subgraph
