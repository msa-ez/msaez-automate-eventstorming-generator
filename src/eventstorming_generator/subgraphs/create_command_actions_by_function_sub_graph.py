import time
import uuid
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import CommandActionGenerationState, ExtractedElementNameDetail, State
from ..utils import JsonUtil, EsActionsUtil, LogUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES, ELEMENT_TYPES
from .worker_subgraphs import create_command_actions_worker_subgraph, command_actions_worker_id_context
from ..config import Config


def resume_from_create_command_actions(state: State):
    try :
        
        state.subgraphs.createCommandActionsByFunctionModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_COMMAND_ACTIONS and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_COMMAND_ACTIONS.__dict__.values():
                LogUtil.add_info_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
                LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Starting command actions generation process")
        return "prepare"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during resume_from_create_command_actions", e)
        state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
        return "complete"

def prepare_command_actions_generation(state: State) -> State:
    """
    Command 액션 생성을 위한 초기 준비 작업 수행
    - 처리할 애그리거트 목록을 구성하고 상태 초기화
    """
    
    try:

        LogUtil.add_info_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Starting command actions generation preparation")
        
        # 처리할 애그리거트 목록 초기화
        pending_generations = []
        total_aggregates = 0
        worker_index = 0
        
        extracted_element_names = state.subgraphs.createElementNamesByDraftsModel.extracted_element_names
        for bc_name, bc_extracted_element_names in extracted_element_names.items():
            for agg_name, agg_extracted_element_name_detail in bc_extracted_element_names.items():
                for element in state.outputs.esValue.elements.values():
                    if (not element or element.get("_type") != ELEMENT_TYPES.AGGREGATE or element.get("name") != agg_name):
                        continue
                    
                    bounded_context_id = element.get("boundedContext", {}).get("id")
                    if not bounded_context_id:
                        continue
                    
                    bounded_context = state.outputs.esValue.elements[bounded_context_id]
                    if not bounded_context or bounded_context.get("name") != bc_name:
                        continue
                    
                    requirement_index_mapping = (state.inputs.draft.metadatas.boundedContextRequirementIndexMapping or {})\
                                                .get(bc_name, None)
                    
                    description = bounded_context.get("description", "")
                    generation_state = CommandActionGenerationState(
                        target_bounded_context_name=bc_name,
                        target_aggregate_name=agg_name,
                        description=description,
                        original_description=description,
                        extracted_element_names=agg_extracted_element_name_detail,
                        requirement_index_mapping=requirement_index_mapping,
                        worker_index=worker_index,
                    )
                    pending_generations.append(generation_state)
                    total_aggregates += 1
                    worker_index += 1

        # 상태 업데이트
        state.subgraphs.createCommandActionsByFunctionModel.pending_generations = pending_generations
        state.subgraphs.createCommandActionsByFunctionModel.is_processing = True
        state.subgraphs.createCommandActionsByFunctionModel.all_complete = len(pending_generations) == 0
        
        LogUtil.add_info_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Preparation completed. Total aggregates to process: {total_aggregates}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during command actions generation preparation", e)
        state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
    
    return state

def select_batch_command_actions(state: State) -> State:
    """
    다음 배치로 처리할 Aggregate들을 선택하는 노드 (병렬 처리용)
    - batch_size만큼의 Aggregate를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """

    try:

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_COMMAND_ACTIONS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_COMMAND_ACTIONS.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createCommandActionsByFunctionModel
        batch_size = Config.get_ai_model_light_max_batch_size()

        # 모든 처리가 완료되었는지 확인
        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        # 현재 처리 중인 배치가 있으면 상태 유지
        if model.current_batch:
            return state
        
        # 대기 중인 Aggregate들에서 배치 크기만큼 선택
        if model.pending_generations:
            # 남은 Aggregate 수와 배치 크기 중 작은 값만큼 선택
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            
            model.current_batch = current_batch
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed to select command actions batch", e)
        state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 Aggregate들을 병렬로 처리
    - 각 Aggregate를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createCommandActionsByFunctionModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        # 워커 서브그래프 인스턴스 생성
        worker_function = create_command_actions_worker_subgraph()
        
        # 각 Aggregate에 대해 워커 ID 생성 및 worker_generations에 저장
        worker_ids = []
        for command_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = command_generation_state
        
        def execute_single_worker(worker_id: str) -> CommandActionGenerationState:
            """
            단일 Aggregate를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                # 현재 스레드의 컨텍스트에 worker_id 설정
                command_actions_worker_id_context.set(worker_id)

                command_generation_state = model.worker_generations[worker_id]
                aggregate_name = command_generation_state.target_aggregate_name
                bc_name = command_generation_state.target_bounded_context_name
                
                # 워커 실행
                result_state = worker_function(state)
                
                # 결과에서 처리된 Command Actions 상태 추출
                completed_command = result_state.subgraphs.createCommandActionsByFunctionModel.worker_generations.get(worker_id)
                
                if completed_command and completed_command.generation_complete:
                    return completed_command
                elif completed_command and completed_command.is_failed:
                    LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER_EXECUTOR] Worker failed for aggregate '{aggregate_name}' in context '{bc_name}'")
                    return completed_command
                else:
                    LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_WORKER_EXECUTOR] Worker returned incomplete result for aggregate '{aggregate_name}' in context '{bc_name}'")
                    command_generation_state.is_failed = True
                    return command_generation_state
                    
            except Exception as e:
                command_generation_state = model.worker_generations.get(worker_id)
                if command_generation_state:
                    aggregate_name = command_generation_state.target_aggregate_name
                    bc_name = command_generation_state.target_bounded_context_name
                    LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER_EXECUTOR] Worker execution failed for aggregate '{aggregate_name}' in context '{bc_name}'", e)
                    command_generation_state.is_failed = True
                    return command_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)
                    # 빈 실패 상태 반환
                    failed_state = CommandActionGenerationState()
                    failed_state.is_failed = True
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
                original_command = model.worker_generations[worker_id]
                try:
                    result_command = future.result()
                    completed_results.append(result_command)
                    
                except Exception as e:
                    aggregate_name = original_command.target_aggregate_name
                    bc_name = original_command.target_bounded_context_name
                    LogUtil.add_exception_object_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Failed to get worker result for aggregate '{aggregate_name}' in context '{bc_name}'", e)
                    original_command.is_failed = True
                    completed_results.append(original_command)
        
        # 결과를 parallel_worker_results에 저장
        model.parallel_worker_results = completed_results
        
        # 사용된 worker_generations 정리 (메모리 절약)
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 Aggregate들의 액션을 ES에 일괄 적용
    - 완료된 Aggregate들을 completed_generations로 이동
    """
    model = state.subgraphs.createCommandActionsByFunctionModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        # 모든 성공한 Aggregate들의 액션 수집
        all_actions = []
        successful_aggregates = []
        failed_aggregates = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for command_result in model.parallel_worker_results:
            aggregate_name = command_result.target_aggregate_name
            bc_name = command_result.target_bounded_context_name
            
            if command_result.generation_complete and command_result.created_actions:
                successful_aggregates.append(command_result)
                all_actions.extend(command_result.created_actions)
            else:
                failed_aggregates.append(command_result)
                LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Aggregate '{aggregate_name}' in context '{bc_name}' failed or has no actions")
        
        # ES 모델에 모든 액션 일괄 적용
        if all_actions:   
            updated_es_value = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(),
                all_actions,
                state.inputs.ids.uid,
                state.inputs.ids.projectId
            )
            
            # 업데이트된 ES 값 저장
            state.outputs.esValue = updated_es_value
            
        # 성공한 Aggregate들을 완료 목록으로 이동 (변수 정리)
        for command_gen in successful_aggregates:
            # 변수 정리
            command_gen.description = ""
            command_gen.original_description = ""
            command_gen.summarized_es_value = {}
            command_gen.created_actions = []
            command_gen.extracted_element_names = ExtractedElementNameDetail()
            
            model.completed_generations.append(command_gen)
        
        # 실패한 Aggregate들도 완료 목록으로 이동 (재시도는 하지 않음)
        for command_gen in failed_aggregates:
            command_gen.description = ""
            command_gen.original_description = ""
            command_gen.summarized_es_value = {}
            command_gen.created_actions = []
            command_gen.extracted_element_names = ExtractedElementNameDetail()
            
            model.completed_generations.append(command_gen)
        
        # 배치 처리 완료 정리
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_aggregates)
        failed_count = len(failed_aggregates)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state

def complete_processing(state: State) -> State:
    """
    모든 처리가 완료되면 최종 상태 업데이트
    """
    model = state.subgraphs.createCommandActionsByFunctionModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[COMMAND_ACTIONS_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_COMMAND_ACTIONS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_COMMAND_ACTIONS.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model.is_processing = False
        model.all_complete = True
        
        completed_count = len(model.completed_generations)
        failed = model.is_failed
        if failed:
            LogUtil.add_error_log(state, f"[COMMAND_ACTIONS_SUBGRAPH] Command actions processing completed with failures. Successfully processed: {completed_count} aggregates")

        if not failed:
            model.completed_generations = []
            model.pending_generations = []

            model.worker_generations = {}
            model.current_batch = []
            model.parallel_worker_results = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during command actions processing completion", e)
        state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
    
    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createCommandActionsByFunctionModel

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
        LogUtil.add_exception_object_log(state, "[COMMAND_ACTIONS_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createCommandActionsByFunctionModel.is_failed = True
        return "complete"

# 서브그래프 생성 함수
def create_command_actions_by_function_subgraph() -> Callable:
    """
    Command 액션 생성 서브그래프 생성 (배치 처리 지원)
    """
    # 서브그래프 정의
    subgraph = StateGraph(State)
    
    # 새로운 배치 처리 노드들 추가
    subgraph.add_node("prepare", prepare_command_actions_generation)
    subgraph.add_node("select_batch", select_batch_command_actions)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)
    
    # 엣지 추가 (새로운 배치 처리 플로우)
    subgraph.add_conditional_edges(START, resume_from_create_command_actions, {
        "prepare": "prepare",
        "select_batch": "select_batch",
        "execute_parallel": "execute_parallel",
        "collect_results": "collect_results",
        "complete": "complete"
    })

    # 새로운 배치 처리 플로우 엣지들
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