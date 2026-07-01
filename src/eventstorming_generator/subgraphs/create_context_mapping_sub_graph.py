import time
import uuid
from typing import Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import (
    ContextMappingGenerationState, State, TextChunkModel, DraftModel
)
from ..utils import JsonUtil, LogUtil, TextChunker, EsTraceUtil, CreateContextMappingUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES
from .worker_subgraphs import create_context_mapping_worker_subgraph, context_mapping_worker_id_context
from ..config import Config


def resume_from_create_context_mapping(state: State):
    try :

        state.subgraphs.createContextMappingModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_CONTEXT_MAPPING and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_CONTEXT_MAPPING.__dict__.values():
                LogUtil.add_info_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createContextMappingModel.is_failed = True
                LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Starting context mapping generation process (parallel mode)")
        return "prepare"

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during resume_from_create_context_mapping", e)
        state.subgraphs.createContextMappingModel.is_failed = True
        return "complete"

def prepare_context_mapping_generation(state: State) -> State:
    """
    Context Mapping 생성을 위한 준비 작업 수행
    """
    
    model = state.subgraphs.createContextMappingModel

    try:
        if model.is_processing:
            return state
        
        model.is_processing = True
        model.all_complete = False
        model.is_merge_completed = False


        merged_bounded_contexts = state.subgraphs.createBoundedContextByFunctionsModel.merged_bounded_contexts
        if not merged_bounded_contexts:
            raise Exception("No merged bounded contexts available for context mapping generation")

        for bounded_context in merged_bounded_contexts:
            model.accumulated_line_number_ranges[bounded_context.name] = []


        pending_generations = []

        # Context Mapping 생성시에는 불필요하게 sparse를 하지 않도록, 0으로 설정
        chunks: List[TextChunkModel] = TextChunker.split_into_chunks_by_line(state.inputs.requirements, Config.get_text_chunker_chunk_size(), 0)

        for index, chunk in enumerate(chunks):
            generation_state = ContextMappingGenerationState(
                requirements=chunk.text,
                line_numbered_requirements=EsTraceUtil.add_line_numbers_to_description(chunk.text, chunk.start_line),
                boundedContexts=merged_bounded_contexts,
                worker_index=index
            )
            pending_generations.append(generation_state)
    
        model.pending_generations = pending_generations
        LogUtil.add_info_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Preparation completed. Total context mappings to process: {len(pending_generations)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during context mapping generation preparation", e)
        model.is_failed = True
    
    return state

def select_batch_context_mapping(state: State) -> State:
    """
    다음 배치로 처리할 Context Mapping들을 선택 (병렬 처리용)
    - batch_size만큼의 Context Mapping를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_CONTEXT_MAPPING
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_CONTEXT_MAPPING.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createContextMappingModel
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
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed to select context mapping batch", e)
        state.subgraphs.createContextMappingModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 Context Mapping들을 병렬로 처리
    - 각 Context Mapping를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createContextMappingModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        worker_function = create_context_mapping_worker_subgraph()
        
        worker_ids = []
        for context_mapping_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = context_mapping_generation_state
        
        def execute_single_worker(worker_id: str) -> ContextMappingGenerationState:
            """
            단일 Context Mapping를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                context_mapping_worker_id_context.set(worker_id)

                context_mapping_generation_state = model.worker_generations[worker_id]
                worker_index = context_mapping_generation_state.worker_index

                result_state = worker_function(state)
                completed_context_mapping = result_state.subgraphs.createContextMappingModel.worker_generations.get(worker_id)
                
                if completed_context_mapping and completed_context_mapping.generation_complete:
                    return completed_context_mapping
                elif completed_context_mapping and completed_context_mapping.is_failed:
                    LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_WORKER_EXECUTOR] Worker failed for context mapping at index {worker_index}")
                    return completed_context_mapping
                else:
                    LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_WORKER_EXECUTOR] Worker returned incomplete result for context mapping at index {worker_index}")
                    context_mapping_generation_state.is_failed = True
                    return context_mapping_generation_state
                    
            except Exception as e:
                context_mapping_generation_state = model.worker_generations.get(worker_id)
                worker_index = context_mapping_generation_state.worker_index
                if context_mapping_generation_state:
                    LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER_EXECUTOR] Worker execution failed for context mapping at index {worker_index}", e)
                    context_mapping_generation_state.is_failed = True
                    return context_mapping_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id} at index {worker_index}", e)
                    failed_state = ContextMappingGenerationState()
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
                original_context_mapping = model.worker_generations[worker_id]
                try:
                    result_context_mapping = future.result()
                    completed_results.append(result_context_mapping)
                    
                except Exception as e:
                    worker_index = original_context_mapping.worker_index
                    LogUtil.add_exception_object_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Failed to get worker result for context mapping at index {worker_index}", e)
                    original_context_mapping.is_failed = True
                    completed_results.append(original_context_mapping)
        model.parallel_worker_results = completed_results
        
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집
    - parallel_worker_results에서 결과 수집
    - 성공한 ContextMapping들을 completed_generations로 이동
    """
    model = state.subgraphs.createContextMappingModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        successful_context_mappings = []
        failed_context_mappings = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for context_mapping_result in model.parallel_worker_results:   
            if context_mapping_result.generation_complete and context_mapping_result.created_context_mappings:
                for context_mapping in context_mapping_result.created_context_mappings:
                    model.accumulated_line_number_ranges[context_mapping.boundedContextName].extend(context_mapping.refs)
                successful_context_mappings.append(context_mapping_result)
            else:
                failed_context_mappings.append(context_mapping_result)
                worker_index = context_mapping_result.worker_index
                LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Context mapping at index {worker_index} failed or has no context mappings")
        
        for context_mapping in successful_context_mappings + failed_context_mappings:
            context_mapping.created_context_mappings = []
            model.completed_generations.append(context_mapping)
        
        model.current_batch = []
        model.parallel_worker_results = []
        if not model.pending_generations:
            model.all_complete = True
        
        successful_count = len(successful_context_mappings)
        failed_count = len(failed_context_mappings)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state


def merge_context_mappings(state: State) -> State:
    """
    수집된 Context Mapping 정보를 병합하여 Draft 메타데이터 생성
    """
    model = state.subgraphs.createContextMappingModel

    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_CONTEXT_MAPPING
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_CONTEXT_MAPPING.MERGE
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        if model.is_merge_completed:
            return state

        if not model.accumulated_line_number_ranges:
            raise Exception("No referenced context mappings found")

        referenced_context_mappings = CreateContextMappingUtil.get_referenced_context_mappings(
            model.accumulated_line_number_ranges,
            state.inputs.requirements
        )

        state.inputs.draft = DraftModel()
        for referenced_context_mapping in referenced_context_mappings:
            state.inputs.draft.metadatas.boundedContextRequirementIndexMapping[referenced_context_mapping.bounded_context_name] = referenced_context_mapping.requirement_index_mapping
            state.inputs.draft.metadatas.boundedContextRequirements[referenced_context_mapping.bounded_context_name] = referenced_context_mapping.created_requirements

        model.accumulated_line_number_ranges = {}
        model.is_merge_completed = True

        LogUtil.add_info_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Merge completed. Total referenced context mappings: {len(referenced_context_mappings)}")

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during merge operation", e)
        model.is_failed = True

    return state

def complete_processing(state: State) -> State:
    """
    Context Mapping 생성 프로세스 완료
    """
    model = state.subgraphs.createContextMappingModel

    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[CONTEXT_MAPPING_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        if not model.is_merge_completed and not model.is_failed:
            LogUtil.add_error_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Referenced context mappings remain unmerged before completion")
            model.is_failed = True

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_CONTEXT_MAPPING
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_CONTEXT_MAPPING.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[CONTEXT_MAPPING_SUBGRAPH] Context mapping generation process completed with failures. Successfully processed: {completed_count} context mappings")

        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during process completion", e)
        model.is_failed = True
    
    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createContextMappingModel

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

        if model.all_complete:
            if not model.is_merge_completed and not model.is_failed:
                return "merge"
            return "complete"

        if not model.is_merge_completed and any(model.accumulated_line_number_ranges.get(name) for name in model.accumulated_line_number_ranges):
            return "merge"
            
        # 아무것도 없으면 완료
        return "complete"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CONTEXT_MAPPING_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createContextMappingModel.is_failed = True
        return "complete"

def create_context_mapping_subgraph() -> Callable:
    """
    Context Mapping 생성 서브그래프 생성
    """
    subgraph = StateGraph(State)
    
    subgraph.add_node("prepare", prepare_context_mapping_generation)
    subgraph.add_node("select_batch", select_batch_context_mapping)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("merge", merge_context_mappings)
    subgraph.add_node("complete", complete_processing)

    subgraph.add_conditional_edges(START, resume_from_create_context_mapping, {
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
            "merge": "merge",
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
