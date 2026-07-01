import time
import uuid
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import ClassIdGenerationState, State
from ..utils import JsonUtil, EsActionsUtil, LogUtil
from ..utils.job_utils import JobUtil
from ..constants import RESUME_NODES
from .worker_subgraphs import create_class_id_worker_subgraph, class_id_worker_id_context
from ..config import Config


def resume_from_create_class_id(state: State):
    try :

        state.subgraphs.createAggregateClassIdByDraftsModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_CLASS_ID and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_CLASS_ID.__dict__.values():
                LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createAggregateClassIdByDraftsModel.is_failed = True
                LogUtil.add_error_log(state, f"[CLASS_ID_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[CLASS_ID_SUBGRAPH] Starting class ID generation process (parallel mode)")
        return "prepare"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during resume_from_create_class_id", e)
        state.subgraphs.createAggregateClassIdByDraftsModel.is_failed = True
        return "complete"

def prepare_class_id_generation(state: State) -> State:
    """
    Aggregate 클래스 ID 생성을 위한 준비 작업 수행
    - 초안 데이터 설정
    - 참조 관계 식별
    - 처리할 참조 목록 초기화
    """
    
    try:
        draft = state.inputs.draft

        if state.subgraphs.createAggregateClassIdByDraftsModel.is_processing:
            return state
        
        state.subgraphs.createAggregateClassIdByDraftsModel.is_processing = True
        state.subgraphs.createAggregateClassIdByDraftsModel.all_complete = False
        
        references = []
        for structure in draft.structures:
            for aggregate_structure in structure.aggregates:
                aggregate_name = aggregate_structure.aggregateName
                for vo in aggregate_structure.valueObjects:
                    if vo.referencedAggregate:
                        ref_aggregate_name = vo.referencedAggregate.name
                        references.append({
                            "fromAggregate": aggregate_name,
                            "toAggregate": ref_aggregate_name,
                            "referenceName": vo.name
                        })
        
        if references:
            processed_pairs = set()
            pending_generations = []
            worker_index = 0

            for ref in references:
                # 양방향 참조를 한 쌍으로 처리하기 위해 정렬된 키 생성
                pair_key = "-".join(sorted([ref["fromAggregate"], ref["toAggregate"]]))
                
                if pair_key not in processed_pairs:
                    processed_pairs.add(pair_key)
                    
                    # 양방향 참조 찾기
                    bidirectional_refs = [
                        r for r in references
                        if (r["fromAggregate"] == ref["fromAggregate"] and r["toAggregate"] == ref["toAggregate"]) or
                           (r["fromAggregate"] == ref["toAggregate"] and r["toAggregate"] == ref["fromAggregate"])
                    ]
                    
                    target_references = [r["referenceName"] for r in bidirectional_refs]
                    
                    related_aggregate_names = set()
                    for structure in draft.structures:
                        for aggregate_structure in structure.aggregates:
                            for vo in aggregate_structure.valueObjects:
                                if vo.name in target_references:
                                    related_aggregate_names.add(vo.referencedAggregate.name)
                                    related_aggregate_names.add(aggregate_structure.aggregateName)

                    specific_draft_options = {}
                    for structure in draft.structures:
                        for aggregate_structure in structure.aggregates:
                            if aggregate_structure.aggregateName in related_aggregate_names:
                                if not specific_draft_options.get(structure.boundedContextName):
                                    specific_draft_options[structure.boundedContextName] = []
                                specific_draft_options[structure.boundedContextName].append({
                                    "aggregate": {
                                        "name": aggregate_structure.aggregateName,
                                        "alias": aggregate_structure.aggregateAlias
                                    },
                                    "valueObjects": [vo for vo in aggregate_structure.valueObjects if vo.name in target_references]
                                })

                    generation_state = ClassIdGenerationState(
                        target_references=target_references,
                        draft_option=specific_draft_options,
                        related_aggregate_names=list(related_aggregate_names),
                        worker_index=worker_index,
                        retry_count=0,
                        generation_complete=False
                    )
                    
                    pending_generations.append(generation_state)
                    worker_index += 1
                    LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Queued class ID generation for references: {', '.join(target_references)} (aggregate pair: {ref['fromAggregate']} <-> {ref['toAggregate']})")
            
            # 처리할 참조 목록 저장
            state.subgraphs.createAggregateClassIdByDraftsModel.pending_generations = pending_generations
            LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Preparation completed. Total class ID generation tasks: {len(pending_generations)}")
        else:
            LogUtil.add_info_log(state, "[CLASS_ID_SUBGRAPH] No aggregate references found, skipping class ID generation")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during class ID generation preparation", e)
        state.subgraphs.createAggregateClassIdByDraftsModel.is_failed = True
    
    return state

def select_batch_class_id(state: State) -> State:
    """
    다음 배치로 처리할 클래스 ID 작업들을 선택 (병렬 처리용)
    - batch_size만큼의 클래스 ID 작업을 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_CLASS_ID
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_CLASS_ID.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createAggregateClassIdByDraftsModel
        batch_size = Config.get_ai_model_light_max_batch_size()

        # 모든 처리가 완료되었는지 확인
        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        # 현재 처리 중인 배치가 있으면 상태 유지
        if model.current_batch:
            return state
        
        # 대기 중인 작업들에서 배치 크기만큼 선택
        if model.pending_generations:
            # 남은 작업 수와 배치 크기 중 작은 값만큼 선택
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            
            model.current_batch = current_batch
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed to select class ID batch", e)
        state.subgraphs.createAggregateClassIdByDraftsModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 클래스 ID 작업들을 병렬로 처리
    - 각 클래스 ID 작업을 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createAggregateClassIdByDraftsModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        # 워커 서브그래프 인스턴스 생성
        worker_function = create_class_id_worker_subgraph()
        
        # 각 클래스 ID 작업에 대해 워커 ID 생성 및 worker_generations에 저장
        worker_ids = []
        for class_id_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = class_id_generation_state
        
        def execute_single_worker(worker_id: str) -> ClassIdGenerationState:
            """
            단일 클래스 ID 작업을 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                # 현재 스레드의 컨텍스트에 worker_id 설정
                class_id_worker_id_context.set(worker_id)

                class_id_generation_state = model.worker_generations[worker_id]
                reference_names = ', '.join(class_id_generation_state.target_references)
                
                # 워커 실행
                result_state = worker_function(state)
                
                # 결과에서 처리된 클래스 ID 상태 추출
                completed_class_id = result_state.subgraphs.createAggregateClassIdByDraftsModel.worker_generations.get(worker_id)
                
                if completed_class_id and completed_class_id.generation_complete:
                    return completed_class_id
                elif completed_class_id and completed_class_id.is_failed:
                    LogUtil.add_error_log(state, f"[CLASS_ID_WORKER_EXECUTOR] Worker failed for class ID references '{reference_names}'")
                    return completed_class_id
                else:
                    LogUtil.add_error_log(state, f"[CLASS_ID_WORKER_EXECUTOR] Worker returned incomplete result for class ID references '{reference_names}'")
                    class_id_generation_state.is_failed = True
                    return class_id_generation_state
                    
            except Exception as e:
                class_id_generation_state = model.worker_generations.get(worker_id)
                if class_id_generation_state:
                    reference_names = ', '.join(class_id_generation_state.target_references)
                    LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER_EXECUTOR] Worker execution failed for class ID references '{reference_names}'", e)
                    class_id_generation_state.is_failed = True
                    return class_id_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[CLASS_ID_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)
                    # 빈 실패 상태 반환
                    failed_state = ClassIdGenerationState()
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
                original_class_id = model.worker_generations[worker_id]
                try:
                    result_class_id = future.result()
                    completed_results.append(result_class_id)
                    
                    reference_names = ', '.join(original_class_id.target_references)
                    
                except Exception as e:
                    reference_names = ', '.join(original_class_id.target_references)
                    LogUtil.add_exception_object_log(state, f"[CLASS_ID_SUBGRAPH] Failed to get worker result for class ID references '{reference_names}'", e)
                    original_class_id.is_failed = True
                    completed_results.append(original_class_id)
        
        # 결과를 parallel_worker_results에 저장
        model.parallel_worker_results = completed_results
        
        # 사용된 worker_generations 정리 (메모리 절약)
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 클래스 ID 작업들의 액션을 ES에 일괄 적용
    - 완료된 작업들을 completed_generations로 이동
    """
    model = state.subgraphs.createAggregateClassIdByDraftsModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        # 모든 성공한 클래스 ID 작업들의 액션 수집
        all_actions = []
        successful_class_ids = []
        failed_class_ids = []
        
        created_references = model.created_references.copy()
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for class_id_result in model.parallel_worker_results:
            reference_names = ', '.join(class_id_result.target_references)
            
            if class_id_result.generation_complete and class_id_result.created_actions:
                filtered_actions = []
                for vo_action in class_id_result.created_actions:
                    if vo_action.objectType != "ValueObject":
                        continue

                    from_aggregate = vo_action.args["fromAggregate"].lower()
                    to_aggregate = vo_action.args["toAggregate"].lower()
                    reference_key = "-".join(sorted([from_aggregate, to_aggregate]))
                    if reference_key not in created_references:
                        created_references.append(reference_key)
                        filtered_actions.append(vo_action)

                        for agg_action in class_id_result.created_actions:
                            if agg_action.objectType != "Aggregate":
                                continue

                            fromValueObjectId = agg_action.args["fromValueObjectId"]
                            if fromValueObjectId == vo_action.ids["valueObjectId"]:
                                filtered_actions.append(agg_action)
                
                if filtered_actions:    
                    successful_class_ids.append(class_id_result)
                    all_actions.extend(filtered_actions)
                else:
                    successful_class_ids.append(class_id_result)
            
            elif class_id_result.generation_complete and not class_id_result.created_actions:
                successful_class_ids.append(class_id_result)
            else:
                failed_class_ids.append(class_id_result)
                LogUtil.add_error_log(state, f"[CLASS_ID_SUBGRAPH] Class ID generation '{reference_names}' failed")
        model.created_references = created_references
        
        # ES 모델에 모든 액션 일괄 적용
        if all_actions:            
            # EsActionsUtil을 사용하여 모든 액션 일괄 적용
            updated_es_value = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(),
                all_actions,
                state.inputs.ids.uid,
                state.inputs.ids.projectId
            )
            
            # 업데이트된 ES 값 저장
            state.outputs.esValue = updated_es_value
            
        # 성공한 클래스 ID 작업들을 완료 목록으로 이동 (변수 정리)
        for class_id in successful_class_ids:
            # 메모리 절약을 위한 변수 정리
            class_id.target_references = []
            class_id.draft_option = {}
            class_id.related_aggregate_names = []
            class_id.summarized_es_value = {}
            class_id.created_actions = []
            
            model.completed_generations.append(class_id)
        
        # 실패한 클래스 ID 작업들도 완료 목록으로 이동 (재시도는 하지 않음)
        for class_id in failed_class_ids:
            class_id.target_references = []
            class_id.draft_option = {}
            class_id.related_aggregate_names = []
            class_id.summarized_es_value = {}
            class_id.created_actions = []
            
            model.completed_generations.append(class_id)
        
        # 배치 처리 완료 정리
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_class_ids)
        failed_count = len(failed_class_ids)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state

def complete_processing(state: State) -> State:
    """
    클래스 ID 생성 프로세스 완료
    """
    model = state.subgraphs.createAggregateClassIdByDraftsModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[CLASS_ID_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_CLASS_ID
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_CLASS_ID.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[CLASS_ID_SUBGRAPH] Class ID generation process completed with failures. Successfully processed: {completed_count} class ID tasks")
        else:
            LogUtil.add_info_log(state, f"[CLASS_ID_SUBGRAPH] Class ID generation process completed successfully. Total processed: {completed_count} class ID tasks")
        
        if not failed:
            model.draft_options = {}
            model.completed_generations = []
            model.pending_generations = []
            model.created_references = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False

    except Exception as e:
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during class ID generation process completion", e)
        model.is_failed = True

    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createAggregateClassIdByDraftsModel

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
        LogUtil.add_exception_object_log(state, "[CLASS_ID_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createAggregateClassIdByDraftsModel.is_failed = True
        return "complete"

def create_aggregate_class_id_by_drafts_subgraph() -> Callable:
    """
    클래스 ID 생성 서브그래프 생성 (병렬 처리 지원)
    """
    # 서브그래프 정의
    subgraph = StateGraph(State)
    
    # 새로운 병렬 처리 노드들 추가
    subgraph.add_node("prepare", prepare_class_id_generation)
    subgraph.add_node("select_batch", select_batch_class_id)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)
    
    # 엣지 추가 (새로운 병렬 처리 플로우)
    subgraph.add_conditional_edges(START, resume_from_create_class_id, {
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