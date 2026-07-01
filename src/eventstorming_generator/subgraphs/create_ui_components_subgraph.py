import time
import uuid
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import CreateUiComponentsGenerationState, State
from ..utils import JsonUtil, LogUtil, EsActionsUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES, ELEMENT_TYPES
from .worker_subgraphs import create_ui_component_worker_subgraph, ui_component_worker_id_context
from ..config import Config


def resume_from_create_ui_components(state: State):
    try :

        state.subgraphs.createUiComponentsModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_UI_COMPONENTS and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_UI_COMPONENTS.__dict__.values():
                LogUtil.add_info_log(state, f"[UI_COMPONENTS_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createUiComponentsModel.is_failed = True
                LogUtil.add_error_log(state, f"[UI_COMPONENTS_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[UI_COMPONENTS_SUBGRAPH] Starting UI Components generation process (parallel mode)")
        return "prepare"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during resume_from_create_ui_components", e)
        state.subgraphs.createUiComponentsModel.is_failed = True
        return "complete"

def prepare_ui_components_generation(state: State) -> State:
    """
    UI 컴포넌트 생성을 위한 준비 작업 수행
    - UI 컴포넌트 탐색
    - 처리할 UI 컴포넌트 목록 초기화
    """
    
    try:
        # 이미 처리 중이면 상태 유지
        if state.subgraphs.createUiComponentsModel.is_processing:
            return state
        
        # 상태 초기화
        state.subgraphs.createUiComponentsModel.is_processing = True
        state.subgraphs.createUiComponentsModel.all_complete = False

        # 처리할 UI 컴포넌트 목록 초기화
        pending_generations = []
        total_ui_components = 0
        
        for index, element in enumerate(state.outputs.esValue.elements.values()):
            if element and element.get("_type") == ELEMENT_TYPES.UI:
                generation_state = CreateUiComponentsGenerationState(
                    target_ui_component=element,
                    ai_request_type="",
                    ai_input_data={},
                    ui_replace_actions=[],
                    worker_index=index,
                    retry_count=0,
                    generation_complete=False,
                    is_failed=False
                )
                
                pending_generations.append(generation_state)
                total_ui_components += 1

        state.subgraphs.createUiComponentsModel.pending_generations = pending_generations
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during UI Components generation preparation", e)
        state.subgraphs.createUiComponentsModel.is_failed = True

    return state

def select_batch_ui_components(state: State) -> State:
    """
    다음 배치로 처리할 UI 컴포넌트들을 선택 (병렬 처리용)
    - batch_size만큼의 UI 컴포넌트를 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_UI_COMPONENTS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_UI_COMPONENTS.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createUiComponentsModel
        batch_size = Config.get_ai_model_light_max_batch_size()

        # 모든 처리가 완료되었는지 확인
        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        # 현재 처리 중인 배치가 있으면 상태 유지
        if model.current_batch:
            return state
        
        # 대기 중인 UI들에서 배치 크기만큼 선택
        if model.pending_generations:
            # 남은 UI 수와 배치 크기 중 작은 값만큼 선택
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            
            model.current_batch = current_batch
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed to select UI batch", e)
        state.subgraphs.createUiComponentsModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 UI 컴포넌트들을 병렬로 처리
    - 각 UI 컴포넌트를 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createUiComponentsModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        # 워커 서브그래프 인스턴스 생성
        worker_function = create_ui_component_worker_subgraph()
        
        # 각 UI에 대해 워커 ID 생성 및 worker_generations에 저장
        worker_ids = []
        for ui_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = ui_generation_state
        
        def execute_single_worker(worker_id: str) -> CreateUiComponentsGenerationState:
            """
            단일 UI 컴포넌트를 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                # 현재 스레드의 컨텍스트에 worker_id 설정
                ui_component_worker_id_context.set(worker_id)

                ui_generation_state = model.worker_generations[worker_id]
                ui_name = ui_generation_state.target_ui_component.get("name", "Unknown")
                
                # 워커 실행
                result_state = worker_function(state)
                
                # 결과에서 처리된 UI 상태 추출
                completed_ui = result_state.subgraphs.createUiComponentsModel.worker_generations.get(worker_id)
                
                if completed_ui and completed_ui.generation_complete:
                    return completed_ui
                elif completed_ui and completed_ui.is_failed:
                    LogUtil.add_error_log(state, f"[UI_WORKER_EXECUTOR] Worker failed for UI component '{ui_name}'")
                    return completed_ui
                else:
                    LogUtil.add_error_log(state, f"[UI_WORKER_EXECUTOR] Worker returned incomplete result for UI component '{ui_name}'")
                    ui_generation_state.is_failed = True
                    return ui_generation_state
                    
            except Exception as e:
                ui_generation_state = model.worker_generations.get(worker_id)
                if ui_generation_state:
                    ui_name = ui_generation_state.target_ui_component.get("name", "Unknown")
                    LogUtil.add_exception_object_log(state, f"[UI_WORKER_EXECUTOR] Worker execution failed for UI component '{ui_name}'", e)
                    ui_generation_state.is_failed = True
                    return ui_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[UI_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)
                    # 빈 실패 상태 반환
                    failed_state = CreateUiComponentsGenerationState()
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
                original_ui = model.worker_generations[worker_id]
                try:
                    result_ui = future.result()
                    completed_results.append(result_ui)

                except Exception as e:
                    ui_name = original_ui.target_ui_component.get("name", "Unknown")
                    LogUtil.add_exception_object_log(state, f"[UI_COMPONENTS_SUBGRAPH] Failed to get worker result for UI component '{ui_name}'", e)
                    original_ui.is_failed = True
                    completed_results.append(original_ui)
        
        # 결과를 parallel_worker_results에 저장
        model.parallel_worker_results = completed_results
        
        # 사용된 worker_generations 정리 (메모리 절약)
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[UI_COMPONENTS_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 UI 컴포넌트들의 액션을 ES에 일괄 적용
    - 완료된 UI들을 completed_generations로 이동
    """
    model = state.subgraphs.createUiComponentsModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        # 모든 성공한 UI 컴포넌트들의 액션 수집
        all_actions = []
        successful_uis = []
        failed_uis = []
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for ui_result in model.parallel_worker_results:
            ui_name = ui_result.target_ui_component.get("name", "Unknown")
            
            if ui_result.generation_complete and ui_result.ui_replace_actions:
                successful_uis.append(ui_result)
                all_actions.extend(ui_result.ui_replace_actions)
            else:
                failed_uis.append(ui_result)
                LogUtil.add_error_log(state, f"[UI_COMPONENTS_SUBGRAPH] UI component '{ui_name}' failed or has no actions")
        
        # ES 모델에 모든 액션 일괄 적용
        if all_actions:   
            updated_es_value = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(),
                all_actions,
                state.inputs.ids.uid,
                state.inputs.ids.projectId
            )
            state.outputs.esValue = updated_es_value
            
        # 성공한 UI들을 완료 목록으로 이동 (변수 정리)
        for ui in successful_uis:
            # 메모리 절약을 위한 변수 정리
            ui.target_ui_component = {}
            ui.ai_request_type = ""
            ui.ai_input_data = {}
            ui.ui_replace_actions = []
            
            model.completed_generations.append(ui)
        
        # 실패한 UI들도 완료 목록으로 이동 (재시도는 하지 않음)
        for ui in failed_uis:
            ui.target_ui_component = {}
            ui.ai_request_type = ""
            ui.ai_input_data = {}
            ui.ui_replace_actions = []
            
            model.completed_generations.append(ui)
        
        # 배치 처리 완료 정리
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_uis)
        failed_count = len(failed_uis)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[UI_COMPONENTS_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state

def complete_processing(state: State) -> State:
    """
    UI Components 생성 프로세스 완료
    """
    model = state.subgraphs.createUiComponentsModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[UI_COMPONENTS_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state
        
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_UI_COMPONENTS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_UI_COMPONENTS.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[UI_COMPONENTS_SUBGRAPH] UI Components generation process completed with failures. Successfully processed: {completed_count} UI component tasks")
        
        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during UI Components generation process completion", e)
        model.is_failed = True

    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createUiComponentsModel

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
        LogUtil.add_exception_object_log(state, "[UI_COMPONENTS_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createUiComponentsModel.is_failed = True
        return "complete"

def create_ui_components_subgraph() -> Callable:
    """
    UI Components 생성 서브그래프 생성 (병렬 처리 지원)
    """
    # 서브그래프 정의
    subgraph = StateGraph(State)
    
    # 새로운 병렬 처리 노드들 추가
    subgraph.add_node("prepare", prepare_ui_components_generation)
    subgraph.add_node("select_batch", select_batch_ui_components)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)
    
    # 엣지 추가 (새로운 병렬 처리 플로우)
    subgraph.add_conditional_edges(START, resume_from_create_ui_components, {
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