import time
import uuid
from typing import Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import (
    BoundedContextGenerationState, State, TextChunkModel
)
from ..utils import JsonUtil, LogUtil, TextChunker
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES
from .worker_subgraphs import create_bounded_context_worker_subgraph, bounded_context_worker_id_context
from ..generators import MergeCreatedBoundedContextGeneratorUtil
from ..config import Config


def resume_from_create_bounded_contexts(state: State):
    try :

        state.subgraphs.createBoundedContextByFunctionsModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_BOUNDED_CONTEXTS and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_BOUNDED_CONTEXTS.__dict__.values():
                LogUtil.add_info_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
                LogUtil.add_error_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Starting bounded context generation process (parallel mode)")
        return "prepare"

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during resume_from_create_bounded_contexts", e)
        state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
        return "complete"

def prepare_bounded_context_generation(state: State) -> State:
    """
    Bounded Context 생성을 위한 준비 작업 수행
    """
    
    try:
        model = state.subgraphs.createBoundedContextByFunctionsModel

        if model.is_processing:
            return state
        
        model.is_processing = True
        model.all_complete = False
        model.is_merge_completed = False
        
        pending_generations = []

        # BoundedContext 생성시에는 불필요하게 sparse를 하지 않도록, 0으로 설정
        chunks: List[TextChunkModel] = TextChunker.split_into_chunks_by_line(state.inputs.requirements, Config.get_text_chunker_chunk_size(), 0)

        for index, chunk in enumerate(chunks):
            generation_state = BoundedContextGenerationState(
                requirements=chunk.text,
                worker_index=index
            )
            pending_generations.append(generation_state)
        
        model.pending_generations = pending_generations
        LogUtil.add_info_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Preparation completed. Total bounded contexts to process: {len(pending_generations)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during bounded context generation preparation", e)
        state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
    
    return state

def select_batch_bounded_contexts(state: State) -> State:
    """
    다음 배치로 처리할 BoundedContext들을 선택 (병렬 처리용)
    - batch_size만큼의 BoundedContext를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_BOUNDED_CONTEXTS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_BOUNDED_CONTEXTS.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createBoundedContextByFunctionsModel
        batch_size = Config.get_ai_model_max_batch_size()

        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        if model.current_batch:
            return state
        
        if model.pending_generations:
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            model.current_batch = current_batch
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed to select bounded context batch", e)
        state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 BoundedContext들을 병렬로 처리
    - 각 BoundedContext를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createBoundedContextByFunctionsModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        worker_function = create_bounded_context_worker_subgraph()
        
        worker_ids = []
        for bounded_context_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = bounded_context_generation_state
        
        def execute_single_worker(worker_id: str) -> BoundedContextGenerationState:
            """
            단일 BoundedContext를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                bounded_context_worker_id_context.set(worker_id)

                bounded_context_generation_state = model.worker_generations[worker_id]
                worker_index = bounded_context_generation_state.worker_index

                result_state = worker_function(state)
                completed_bounded_context = result_state.subgraphs.createBoundedContextByFunctionsModel.worker_generations.get(worker_id)
                
                if completed_bounded_context and completed_bounded_context.generation_complete:
                    return completed_bounded_context
                elif completed_bounded_context and completed_bounded_context.is_failed:
                    LogUtil.add_error_log(state, f"[BOUNDED_CONTEXT_WORKER_EXECUTOR] Worker failed for bounded context at index {worker_index}")
                    return completed_bounded_context
                else:
                    LogUtil.add_error_log(state, f"[BOUNDED_CONTEXT_WORKER_EXECUTOR] Worker returned incomplete result for bounded context at index {worker_index}")
                    bounded_context_generation_state.is_failed = True
                    return bounded_context_generation_state
                    
            except Exception as e:
                bounded_context_generation_state = model.worker_generations.get(worker_id)
                worker_index = bounded_context_generation_state.worker_index
                if bounded_context_generation_state:
                    LogUtil.add_exception_object_log(state, f"[BOUNDED_CONTEXT_WORKER_EXECUTOR] Worker execution failed for bounded context at index {worker_index}", e)
                    bounded_context_generation_state.is_failed = True
                    return bounded_context_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[BOUNDED_CONTEXT_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id} at index {worker_index}", e)

                    failed_state = BoundedContextGenerationState()
                    failed_state.is_failed = True
                    return failed_state
        
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            future_to_worker_id = {
                executor.submit(execute_single_worker, worker_id): worker_id 
                for worker_id in worker_ids
            }
            
            completed_results = []
            for future in as_completed(future_to_worker_id):
                worker_id = future_to_worker_id[future]
                original_bounded_context = model.worker_generations[worker_id]
                try:
                    result_bounded_context = future.result()
                    completed_results.append(result_bounded_context)
                    
                except Exception as e:
                    worker_index = original_bounded_context.worker_index
                    LogUtil.add_exception_object_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Failed to get worker result for bounded context at index {worker_index}", e)
                    original_bounded_context.is_failed = True
                    completed_results.append(original_bounded_context)
        model.parallel_worker_results = completed_results
        
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 BoundedContext들을 completed_generations로 이동
    """
    model = state.subgraphs.createBoundedContextByFunctionsModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        successful_bounded_contexts = []
        failed_bounded_contexts = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for bounded_context_result in model.parallel_worker_results:   
            if bounded_context_result.generation_complete and bounded_context_result.created_bounded_contexts:
                successful_bounded_contexts.append(bounded_context_result)
                model.accumulated_bounded_contexts.extend(bounded_context_result.created_bounded_contexts)
            else:
                failed_bounded_contexts.append(bounded_context_result)
                worker_index = bounded_context_result.worker_index
                LogUtil.add_error_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Bounded context at index {worker_index} failed or has no bounded contexts")
        
        for bounded_context in successful_bounded_contexts + failed_bounded_contexts:
            bounded_context.created_bounded_contexts = []
            model.completed_generations.append(bounded_context)
        
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_bounded_contexts)
        failed_count = len(failed_bounded_contexts)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state


def merge_bounded_contexts(state: State) -> State:
    """
    수집된 Bounded Context들을 병합하여 최종 결과 생성
    """
    model = state.subgraphs.createBoundedContextByFunctionsModel

    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_BOUNDED_CONTEXTS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_BOUNDED_CONTEXTS.MERGE
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        if model.is_merge_completed:
            return state

        if not model.accumulated_bounded_contexts:
            raise Exception("No accumulated bounded contexts found")

        model.accumulated_bounded_contexts = sorted(
            model.accumulated_bounded_contexts, key=lambda x: x.name
        )

        model.merged_bounded_contexts = MergeCreatedBoundedContextGeneratorUtil.merge_created_bounded_context_safely(
            model.accumulated_bounded_contexts,
            Config.get_ai_model(),
            state.inputs.preferedLanguage,
            model.max_retry_count,
            state.inputs.jobId
        )

        if model.merged_bounded_contexts:
            model.merged_bounded_contexts = sorted(
                model.merged_bounded_contexts, key=lambda x: x.name
            )

        model.accumulated_bounded_contexts = []
        model.is_merge_completed = True

        LogUtil.add_info_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Merge completed. Total merged bounded contexts: {len(model.merged_bounded_contexts)}")

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during merge operation", e)
        model.is_failed = True

    return state

def complete_processing(state: State) -> State:
    """
    Bounded Context 생성 프로세스 완료
    """
    model = state.subgraphs.createBoundedContextByFunctionsModel

    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[BOUNDED_CONTEXT_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        if not model.merged_bounded_contexts and model.accumulated_bounded_contexts and not model.is_failed:
            LogUtil.add_error_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Accumulated bounded contexts remain unmerged before completion")
            model.is_failed = True

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_BOUNDED_CONTEXTS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_BOUNDED_CONTEXTS.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[BOUNDED_CONTEXT_SUBGRAPH] Bounded context generation process completed with failures. Successfully processed: {completed_count} bounded contexts")

        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during process completion", e)
        state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
    
    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createBoundedContextByFunctionsModel

        if model.is_failed:
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

        # 모든 배치 처리가 완료되었고 병합이 필요한 경우 병합 단계로 이동
        if model.all_complete:
            if not model.is_merge_completed and not model.merged_bounded_contexts:
                return "merge"
            return "complete"

        if not model.is_merge_completed and model.accumulated_bounded_contexts:
            return "merge"
            
        # 아무것도 없으면 완료
        return "complete"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[BOUNDED_CONTEXT_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createBoundedContextByFunctionsModel.is_failed = True
        return "complete"

def create_bounded_context_by_functions_subgraph() -> Callable:
    """
    Bounded Context 생성 서브그래프 생성
    """
    subgraph = StateGraph(State)
    
    subgraph.add_node("prepare", prepare_bounded_context_generation)
    subgraph.add_node("select_batch", select_batch_bounded_contexts)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("merge", merge_bounded_contexts)
    subgraph.add_node("complete", complete_processing)

    subgraph.add_conditional_edges(START, resume_from_create_bounded_contexts, {
        "prepare": "prepare",
        "select_batch": "select_batch",
        "execute_parallel": "execute_parallel",
        "collect_results": "collect_results",
        "merge": "merge",
        "complete": "complete"
    })

    subgraph.add_conditional_edges(
        "prepare",
        decide_next_step,
        {
            "select_batch": "select_batch",
            "merge": "merge",
            "complete": "complete"
        }
    )
    
    subgraph.add_conditional_edges(
        "select_batch",
        decide_next_step,
        {
            "select_batch": "select_batch",
            "execute_parallel": "execute_parallel", 
            "merge": "merge",
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
            "merge": "merge",
            "complete": "complete"
        }
    )

    subgraph.add_conditional_edges(
        "merge",
        decide_next_step,
        {
            "complete": "complete"
        }
    )
    

    compiled_subgraph = subgraph.compile()
    def run_subgraph(state: State) -> State:
        result = State(**compiled_subgraph.invoke(state, {"recursion_limit": 2147483647}))
        return result
    
    return run_subgraph