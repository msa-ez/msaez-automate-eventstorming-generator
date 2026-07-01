import time
import uuid
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import (
    AggregateGenerationState, State
)
from ..utils import JsonUtil, EsActionsUtil, LogUtil, CaseConvertUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES
from .worker_subgraphs import create_aggregate_worker_subgraph, aggregate_worker_id_context
from ..config import Config


def resume_from_create_aggregates(state: State):
    try :

        state.subgraphs.createAggregateByFunctionsModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_AGGREGATES and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_AGGREGATES.__dict__.values():
                LogUtil.add_info_log(state, f"[AGGREGATE_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createAggregateByFunctionsModel.is_failed = True
                LogUtil.add_error_log(state, f"[AGGREGATE_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[AGGREGATE_SUBGRAPH] Starting aggregate generation process (parallel mode)")
        return "prepare"

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during resume_from_create_aggregates", e)
        state.subgraphs.createAggregateByFunctionsModel.is_failed = True
        return "complete"

def prepare_aggregate_generation(state: State) -> State:
    """
    초안으로부터 Aggregate 생성을 위한 준비 작업 수행
    - 초안 데이터 설정
    - DDL 필드 추출 및 애그리거트별 할당
    - 처리할 Aggregate 목록 초기화
    """
    
    try:
        if state.subgraphs.createAggregateByFunctionsModel.is_processing:
            return state
        
        state.subgraphs.createAggregateByFunctionsModel.is_processing = True
        state.subgraphs.createAggregateByFunctionsModel.all_complete = False
        
        pending_generations = []
        worker_index = 0
        for structure in state.inputs.draft.structures:
            target_bounded_context_name = structure.boundedContextName
            description = state.inputs.draft.metadatas.boundedContextRequirements.get(target_bounded_context_name, "")

            for aggregate_structure in structure.aggregates:
                target_aggregate_name = aggregate_structure.aggregateName

                attributes_to_generate = (state.inputs.draft.additionalRequests.essentialAggregateAttributes or {})\
                                            .get(target_bounded_context_name, {}).get(target_aggregate_name, [])
                attributes_to_generate = [CaseConvertUtil.camel_case(attribute) for attribute in attributes_to_generate]

                requirement_index_mapping = (state.inputs.draft.metadatas.boundedContextRequirementIndexMapping or {})\
                                                .get(target_bounded_context_name, None)
                
                generation_state = AggregateGenerationState(
                    target_bounded_context_name=target_bounded_context_name,
                    target_aggregate_structure=aggregate_structure,
                    description=description,
                    original_description=description,
                    attributes_to_generate=attributes_to_generate,
                    requirement_index_mapping=requirement_index_mapping,
                    worker_index=worker_index
                )
                pending_generations.append(generation_state)
                worker_index += 1
        
        state.subgraphs.createAggregateByFunctionsModel.pending_generations = pending_generations
        LogUtil.add_info_log(state, f"[AGGREGATE_SUBGRAPH] Preparation completed. Total aggregates to process: {len(pending_generations)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during aggregate generation preparation", e)
        state.subgraphs.createAggregateByFunctionsModel.is_failed = True
    
    return state

def select_batch_aggregates(state: State) -> State:
    """
    다음 배치로 처리할 Aggregate들을 선택 (병렬 처리용)
    - batch_size만큼의 Aggregate를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_AGGREGATES
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_AGGREGATES.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createAggregateByFunctionsModel
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
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed to select aggregate batch", e)
        state.subgraphs.createAggregateByFunctionsModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 Aggregate들을 병렬로 처리
    - 각 Aggregate를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createAggregateByFunctionsModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        worker_function = create_aggregate_worker_subgraph()
        
        worker_ids = []
        for aggregate_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = aggregate_generation_state
        
        def execute_single_worker(worker_id: str) -> AggregateGenerationState:
            """
            단일 Aggregate를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                aggregate_worker_id_context.set(worker_id)

                aggregate_generation_state = model.worker_generations[worker_id]
                aggregate_name = aggregate_generation_state.target_aggregate_structure.aggregateName
                
                result_state = worker_function(state)
                completed_aggregate = result_state.subgraphs.createAggregateByFunctionsModel.worker_generations.get(worker_id)
                
                if completed_aggregate and completed_aggregate.generation_complete:
                    return completed_aggregate
                elif completed_aggregate and completed_aggregate.is_failed:
                    LogUtil.add_error_log(state, f"[AGGREGATE_WORKER_EXECUTOR] Worker failed for aggregate '{aggregate_name}'")
                    return completed_aggregate
                else:
                    LogUtil.add_error_log(state, f"[AGGREGATE_WORKER_EXECUTOR] Worker returned incomplete result for aggregate '{aggregate_name}'")
                    aggregate_generation_state.is_failed = True
                    return aggregate_generation_state
                    
            except Exception as e:
                aggregate_generation_state = model.worker_generations.get(worker_id)
                if aggregate_generation_state:
                    aggregate_name = aggregate_generation_state.target_aggregate_structure.aggregateName
                    LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER_EXECUTOR] Worker execution failed for aggregate '{aggregate_name}'", e)
                    aggregate_generation_state.is_failed = True
                    return aggregate_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[AGGREGATE_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)

                    failed_state = AggregateGenerationState()
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
                original_aggregate = model.worker_generations[worker_id]
                try:
                    result_aggregate = future.result()
                    completed_results.append(result_aggregate)
                    
                except Exception as e:
                    aggregate_name = original_aggregate.target_aggregate_structure.aggregateName
                    LogUtil.add_exception_object_log(state, f"[AGGREGATE_SUBGRAPH] Failed to get worker result for aggregate '{aggregate_name}'", e)
                    original_aggregate.is_failed = True
                    completed_results.append(original_aggregate)
        model.parallel_worker_results = completed_results
        
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[AGGREGATE_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 Aggregate들의 액션을 ES에 일괄 적용
    - 완료된 Aggregate들을 completed_generations로 이동
    """
    model = state.subgraphs.createAggregateByFunctionsModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        all_actions = []
        successful_aggregates = []
        failed_aggregates = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for aggregate_result in model.parallel_worker_results:   
            if aggregate_result.generation_complete and aggregate_result.created_actions:
                successful_aggregates.append(aggregate_result)
                all_actions.extend([action for action in aggregate_result.created_actions])
            else:
                failed_aggregates.append(aggregate_result)
                aggregate_name = aggregate_result.target_aggregate_structure.aggregateName
                LogUtil.add_error_log(state, f"[AGGREGATE_SUBGRAPH] Aggregate '{aggregate_name}' failed or has no actions")
        
        if all_actions:   
            state.outputs.esValue = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(),
                all_actions,
                state.inputs.ids.uid,
                state.inputs.ids.projectId
            )
            
        for aggregate in successful_aggregates + failed_aggregates:
            if aggregate.target_aggregate_structure:
                aggregate.target_aggregate_structure.enumerations = []
                aggregate.target_aggregate_structure.valueObjects = []

            aggregate.description = ""
            aggregate.original_description = ""
            aggregate.attributes_to_generate = []
            aggregate.missing_attributes = []
            aggregate.created_actions = []
            model.completed_generations.append(aggregate)
        
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_aggregates)
        failed_count = len(failed_aggregates)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[AGGREGATE_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state

def complete_processing(state: State) -> State:
    """
    Aggregate 생성 프로세스 완료
    """
    model = state.subgraphs.createAggregateByFunctionsModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[AGGREGATE_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state


        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_AGGREGATES
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_AGGREGATES.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[AGGREGATE_SUBGRAPH] Aggregate generation process completed with failures. Successfully processed: {completed_count} aggregates")

        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during process completion", e)
        model.is_failed = True
    
    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createAggregateByFunctionsModel

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
        LogUtil.add_exception_object_log(state, "[AGGREGATE_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createAggregateByFunctionsModel.is_failed = True
        return "complete"

def create_aggregate_by_functions_subgraph() -> Callable:
    """
    Aggregate 생성 서브그래프 생성
    """
    subgraph = StateGraph(State)
    
    subgraph.add_node("prepare", prepare_aggregate_generation)
    subgraph.add_node("select_batch", select_batch_aggregates)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)

    subgraph.add_conditional_edges(START, resume_from_create_aggregates, {
        "prepare": "prepare",
        "select_batch": "select_batch",
        "execute_parallel": "execute_parallel",
        "collect_results": "collect_results",
        "complete": "complete"
    })

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
    

    compiled_subgraph = subgraph.compile()
    def run_subgraph(state: State) -> State:
        result = State(**compiled_subgraph.invoke(state, {"recursion_limit": 2147483647}))
        return result
    
    return run_subgraph