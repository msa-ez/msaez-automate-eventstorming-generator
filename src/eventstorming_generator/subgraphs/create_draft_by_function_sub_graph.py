import time
import uuid
from typing import Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import (
    DraftGenerationState, State
)
from ..utils import JsonUtil, LogUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES
from .worker_subgraphs import create_draft_worker_subgraph, draft_worker_id_context
from ..generators import MergeDraftGeneratorUtil
from ..config import Config


def resume_from_create_drafts(state: State):
    try :

        state.subgraphs.createDraftByFunctionModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_DRAFT_BY_FUNCTION and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_DRAFT_BY_FUNCTION.__dict__.values():
                LogUtil.add_info_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createDraftByFunctionModel.is_failed = True
                LogUtil.add_error_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Starting draft generation process (parallel mode)")
        return "prepare"
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during resume_from_create_drafts", e)
        state.subgraphs.createDraftByFunctionModel.is_failed = True
        return "complete"

def prepare_draft_generation(state: State) -> State:
    """
    Draft 생성을 위한 준비 작업 수행
    """
    
    model = state.subgraphs.createDraftByFunctionModel

    try:
        if model.is_processing:
            return state
        
        model.is_processing = True
        model.all_complete = False
        model.is_merge_completed = False

        merged_bounded_contexts = state.subgraphs.createBoundedContextByFunctionsModel.merged_bounded_contexts
        bounded_context_requirements = state.inputs.draft.metadatas.boundedContextRequirements

        if not bounded_context_requirements or not merged_bounded_contexts:
            raise Exception("No bounded context requirements or merged bounded contexts found")
        
        pending_generations = []

        for index, bc_info in enumerate(merged_bounded_contexts):
            requirements = bounded_context_requirements.get(bc_info.name, "")
            if not requirements:
                LogUtil.add_warning_log(state, f"No requirements found for bounded context: {bc_info.name}")
                continue
            
            generation_state = DraftGenerationState(
                bounded_context_info=bc_info,
                requirements=requirements,
                worker_index=index
            )
            pending_generations.append(generation_state)
        
        model.pending_generations = pending_generations
        LogUtil.add_info_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Preparation completed. Total drafts to process: {len(pending_generations)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during draft generation preparation", e)
        state.subgraphs.createDraftByFunctionModel.is_failed = True
    
    return state

def select_batch_drafts(state: State) -> State:
    """
    다음 배치로 처리할 Draft들을 선택 (병렬 처리용)
    - batch_size만큼의 Draft를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_DRAFT_BY_FUNCTION
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_DRAFT_BY_FUNCTION.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createDraftByFunctionModel
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
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed to select draft batch", e)
        state.subgraphs.createDraftByFunctionModel.is_failed = True
    
    return state

def execute_parallel_drafts(state: State) -> State:
    """
    현재 배치의 Draft들을 병렬로 처리
    - 각 Draft를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createDraftByFunctionModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        worker_function = create_draft_worker_subgraph()
        
        worker_ids = []
        for draft_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = draft_generation_state
        
        def execute_single_worker(worker_id: str) -> DraftGenerationState:
            """
            단일 Draft를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                draft_worker_id_context.set(worker_id)

                draft_generation_state = model.worker_generations[worker_id]
                worker_index = draft_generation_state.worker_index

                result_state = worker_function(state)
                completed_draft = result_state.subgraphs.createDraftByFunctionModel.worker_generations.get(worker_id)
                
                if completed_draft and completed_draft.generation_complete:
                    return completed_draft
                elif completed_draft and completed_draft.is_failed:
                    LogUtil.add_error_log(state, f"[DRAFT_BY_FUNCTION_WORKER_EXECUTOR] Worker failed for draft at index {worker_index}")
                    return completed_draft
                else:
                    LogUtil.add_error_log(state, f"[DRAFT_BY_FUNCTION_WORKER_EXECUTOR] Worker returned incomplete result for draft at index {worker_index}")
                    draft_generation_state.is_failed = True
                    return draft_generation_state
                    
            except Exception as e:
                draft_generation_state = model.worker_generations.get(worker_id)
                worker_index = draft_generation_state.worker_index
                if draft_generation_state:
                    LogUtil.add_exception_object_log(state, f"[DRAFT_BY_FUNCTION_WORKER_EXECUTOR] Worker execution failed for draft at index {worker_index}", e)
                    draft_generation_state.is_failed = True
                    return draft_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[DRAFT_BY_FUNCTION_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id} at index {worker_index}", e)

                    failed_state = DraftGenerationState()
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
                original_draft = model.worker_generations[worker_id]
                try:
                    result_draft = future.result()
                    completed_results.append(result_draft)
                    
                except Exception as e:
                    worker_index = original_draft.worker_index
                    LogUtil.add_exception_object_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Failed to get worker result for draft at index {worker_index}", e)
                    original_draft.is_failed = True
                    completed_results.append(original_draft)
        model.parallel_worker_results = completed_results
        
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_drafts(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 Draft를 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 Draft들을 completed_generations로 이동
    """
    model = state.subgraphs.createDraftByFunctionModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        successful_drafts: List[DraftGenerationState] = []
        failed_drafts: List[DraftGenerationState] = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for draft_result in model.parallel_worker_results:   
            if draft_result.generation_complete and draft_result.created_draft:
                successful_drafts.append(draft_result)
                model.accumulated_drafts.append(draft_result.created_draft)
            else:
                failed_drafts.append(draft_result)
                worker_index = draft_result.worker_index
                LogUtil.add_error_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Draft at index {worker_index} failed or has no draft")
        
        for draft in successful_drafts + failed_drafts:
            draft.created_draft = None
            model.completed_generations.append(draft)
        
        model.current_batch = []
        model.parallel_worker_results = []
        if not model.pending_generations:
            model.all_complete = True
        
        successful_count = len(successful_drafts)
        failed_count = len(failed_drafts)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state


def merge_drafts(state: State) -> State:
    """
    수집된 Draft들을 병합하여 최종 구조를 생성
    """
    model = state.subgraphs.createDraftByFunctionModel

    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_DRAFT_BY_FUNCTION
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_DRAFT_BY_FUNCTION.MERGE
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        if model.is_merge_completed:
            return state

        if not model.accumulated_drafts:
            raise Exception("No accumulated drafts found")

        model.accumulated_drafts = sorted(
            model.accumulated_drafts, key=lambda x: x.boundedContextName
        )

        state.inputs.draft.structures = MergeDraftGeneratorUtil.sequential_merge_drafts_safely(
            model.accumulated_drafts,
            Config.get_ai_model(),
            state.inputs.preferedLanguage,
            5,
            3,
            state.inputs.jobId
        )

        if state.inputs.draft.structures:
            state.inputs.draft.structures = sorted(
                state.inputs.draft.structures, key=lambda x: x.boundedContextName
            )

        model.accumulated_drafts = []
        model.is_merge_completed = True

        LogUtil.add_info_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Merge completed. Total merged drafts: {len(state.inputs.draft.structures)}")

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during merge operation", e)
        model.is_failed = True

    return state

def complete_processing(state: State) -> State:
    """
    Bounded Context 생성 프로세스 완료
    """
    model = state.subgraphs.createDraftByFunctionModel

    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[DRAFT_BY_FUNCTION_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        if not model.is_merge_completed and not model.is_failed:
            LogUtil.add_error_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Accumulated drafts remain unmerged before completion")
            model.is_failed = True

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_DRAFT_BY_FUNCTION
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_DRAFT_BY_FUNCTION.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[DRAFT_BY_FUNCTION_SUBGRAPH] Draft generation process completed with failures. Successfully processed: {completed_count} drafts")

        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during process completion", e)
        model.is_failed = True
    
    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createDraftByFunctionModel

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

        if not model.is_merge_completed and model.accumulated_drafts:
            return "merge"
            
        # 아무것도 없으면 완료
        return "complete"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[DRAFT_BY_FUNCTION_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createDraftByFunctionModel.is_failed = True
        return "complete"

def create_draft_by_function_subgraph() -> Callable:
    """
    Draft 생성 서브그래프 생성
    """
    subgraph = StateGraph(State)
    
    subgraph.add_node("prepare", prepare_draft_generation)
    subgraph.add_node("select_batch", select_batch_drafts)
    subgraph.add_node("execute_parallel", execute_parallel_drafts)
    subgraph.add_node("collect_results", collect_and_apply_drafts)
    subgraph.add_node("merge", merge_drafts)
    subgraph.add_node("complete", complete_processing)

    subgraph.add_conditional_edges(START, resume_from_create_drafts, {
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