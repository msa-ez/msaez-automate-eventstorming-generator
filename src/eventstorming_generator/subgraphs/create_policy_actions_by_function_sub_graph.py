import time
import uuid
from typing import Callable, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from langgraph.graph import StateGraph, START

from ..models import PolicyActionGenerationState, ActionModel, State
from ..utils import JsonUtil, EsActionsUtil, LogUtil, CaseConvertUtil, EsAliasTransManager, EsTraceUtil
from ..utils.job_utils import JobUtil
from .worker_subgraphs import create_policy_actions_worker_subgraph, policy_actions_worker_id_context
from ..constants import RESUME_NODES
from ..config import Config


def resume_from_create_policy_actions(state: State):
    try :

        state.subgraphs.createPolicyActionsByFunctionModel.start_time = time.time()
        if state.outputs.lastCompletedRootGraphNode == RESUME_NODES.ROOT_GRAPH.CREATE_POLICY_ACTIONS and \
           state.outputs.lastCompletedSubGraphNode:
            if state.outputs.lastCompletedSubGraphNode in RESUME_NODES.CREATE_POLICY_ACTIONS.__dict__.values():
                LogUtil.add_info_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Resuming from checkpoint: '{state.outputs.lastCompletedSubGraphNode}'")
                return state.outputs.lastCompletedSubGraphNode
            else:
                state.subgraphs.createPolicyActionsByFunctionModel.is_failed = True
                LogUtil.add_error_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Invalid checkpoint node: '{state.outputs.lastCompletedSubGraphNode}'")
                return "complete"
        
        LogUtil.add_info_log(state, "[POLICY_ACTIONS_SUBGRAPH] Starting policy actions generation process (parallel mode)")
        return "prepare"
    
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during resume_from_create_policy_actions", e)
        state.subgraphs.createPolicyActionsByFunctionModel.is_failed = True
        return "complete"

def prepare_policy_actions_generation(state: State) -> State:
    """
    초안으로부터 Policy 액션 생성을 위한 준비 작업 수행
    - 초안 데이터 설정
    - 처리할 Policy 액션 목록 초기화
    """
    
    try:

        LogUtil.add_info_log(state, "[POLICY_ACTIONS_SUBGRAPH] Starting policy actions generation preparation")

        # 이미 처리 중이면 상태 유지
        if state.subgraphs.createPolicyActionsByFunctionModel.is_processing:
            return state
        
        state.subgraphs.createPolicyActionsByFunctionModel.is_processing = True
        state.subgraphs.createPolicyActionsByFunctionModel.all_complete = False
        state.subgraphs.createPolicyActionsByFunctionModel.completed_generations = []
        
        pending_generations = []
        for index, structure in enumerate(state.inputs.draft.structures):
            bc_name = structure.boundedContextName
            description = state.inputs.draft.metadatas.boundedContextRequirements.get(bc_name, "")

            requirement_index_mapping = (state.inputs.draft.metadatas.boundedContextRequirementIndexMapping or {})\
                                            .get(bc_name, None)

            generation_state = PolicyActionGenerationState(
                target_bounded_context_name=bc_name,
                description=description,
                original_description=description,
                requirement_index_mapping=requirement_index_mapping,
                worker_index=index,
                retry_count=0,
                generation_complete=False
            )
            pending_generations.append(generation_state)
        state.subgraphs.createPolicyActionsByFunctionModel.pending_generations = pending_generations
        
        LogUtil.add_info_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Preparation completed. Total bounded contexts to process: {len(pending_generations)}")
        
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during policy actions generation preparation", e)
        state.subgraphs.createPolicyActionsByFunctionModel.is_failed = True
    
    return state

def select_batch_policy_actions(state: State) -> State:
    """
    다음 배치로 처리할 Policy 액션들을 선택 (병렬 처리용)
    - batch_size만큼의 Policy 액션을 한 번에 선택
    - current_batch에 설정하여 병렬 처리 준비
    """
    
    try:
        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_POLICY_ACTIONS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_POLICY_ACTIONS.SELECT_BATCH
        JobUtil.update_job_to_firebase_fire_and_forget(state)

        model = state.subgraphs.createPolicyActionsByFunctionModel
        batch_size = Config.get_ai_model_max_batch_size()

        # 모든 처리가 완료되었는지 확인
        if not model.pending_generations and not model.current_batch:
            model.all_complete = True
            model.is_processing = False
            return state
        
        # 현재 처리 중인 배치가 있으면 상태 유지
        if model.current_batch:
            return state
        
        # 대기 중인 Policy들에서 배치 크기만큼 선택
        if model.pending_generations:
            # 남은 Policy 수와 배치 크기 중 작은 값만큼 선택
            actual_batch_size = min(batch_size, len(model.pending_generations))
            
            current_batch = []
            for _ in range(actual_batch_size):
                if model.pending_generations:
                    current_batch.append(model.pending_generations.pop(0))
            
            model.current_batch = current_batch

    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed to select policy batch", e)
        state.subgraphs.createPolicyActionsByFunctionModel.is_failed = True
    
    return state

def execute_parallel_workers(state: State) -> State:
    """
    현재 배치의 Policy 액션들을 병렬로 처리
    - 각 Policy 액션을 개별 워커 서브그래프에서 병렬 실행
    - ThreadPoolExecutor를 사용하여 동시 처리
    """
    model = state.subgraphs.createPolicyActionsByFunctionModel
    
    if not model.current_batch:
        return state
    
    batch_size = len(model.current_batch)

    try:
        # 워커 서브그래프 인스턴스 생성
        worker_function = create_policy_actions_worker_subgraph()
        
        # 각 Policy에 대해 워커 ID 생성 및 worker_generations에 저장
        worker_ids = []
        for policy_generation_state in model.current_batch:
            worker_id = str(uuid.uuid4())
            worker_ids.append(worker_id)
            model.worker_generations[worker_id] = policy_generation_state
        
        def execute_single_worker(worker_id: str) -> PolicyActionGenerationState:
            """
            단일 Policy 액션을 워커에서 처리하는 함수 (메모리 최적화 버전)
            """
            try:
                # 현재 스레드의 컨텍스트에 worker_id 설정
                policy_actions_worker_id_context.set(worker_id)

                policy_generation_state = model.worker_generations[worker_id]
                bc_name = policy_generation_state.target_bounded_context_name
                
                # 워커 실행
                result_state = worker_function(state)
                
                # 결과에서 처리된 Policy 상태 추출
                completed_policy = result_state.subgraphs.createPolicyActionsByFunctionModel.worker_generations.get(worker_id)
                
                if completed_policy and completed_policy.generation_complete:
                    return completed_policy
                elif completed_policy and completed_policy.is_failed:
                    LogUtil.add_error_log(state, f"[POLICY_WORKER_EXECUTOR] Worker failed for Policy bounded context '{bc_name}'")
                    return completed_policy
                else:
                    LogUtil.add_error_log(state, f"[POLICY_WORKER_EXECUTOR] Worker returned incomplete result for Policy bounded context '{bc_name}'")
                    policy_generation_state.is_failed = True
                    return policy_generation_state
                    
            except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
                policy_generation_state = model.worker_generations.get(worker_id)
                if policy_generation_state:
                    bc_name = policy_generation_state.target_bounded_context_name
                    LogUtil.add_exception_object_log(state, f"[POLICY_WORKER_EXECUTOR] Worker execution failed for Policy bounded context '{bc_name}'", e)
                    policy_generation_state.is_failed = True
                    return policy_generation_state
                else:
                    LogUtil.add_exception_object_log(state, f"[POLICY_WORKER_EXECUTOR] Worker execution failed for unknown worker_id: {worker_id}", e)
                    # 빈 실패 상태 반환
                    failed_state = PolicyActionGenerationState()
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
                original_policy = model.worker_generations[worker_id]
                try:
                    result_policy = future.result()
                    completed_results.append(result_policy)
                                   
                except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
                    bc_name = original_policy.target_bounded_context_name
                    LogUtil.add_exception_object_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Failed to get worker result for Policy bounded context '{bc_name}'", e)
                    original_policy.is_failed = True
                    completed_results.append(original_policy)
        
        # 결과를 parallel_worker_results에 저장
        model.parallel_worker_results = completed_results
        
        # 사용된 worker_generations 정리 (메모리 절약)
        for worker_id in worker_ids:
            if worker_id in model.worker_generations:
                del model.worker_generations[worker_id]
        
        successful_count = sum(1 for result in completed_results if result.generation_complete)
        failed_count = sum(1 for result in completed_results if result.is_failed)
        
        LogUtil.add_info_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Parallel execution completed. Successful: {successful_count}, Failed: {failed_count}, Total: {len(completed_results)}")
        
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during parallel worker execution", e)
        model.is_failed = True
    
    return state

def collect_and_apply_results(state: State) -> State:
    """
    병렬 워커들의 결과를 수집하고 ES 모델에 적용
    - parallel_worker_results에서 결과 수집
    - 성공한 Policy 액션들의 액션을 ES에 일괄 적용
    - 완료된 Policy들을 completed_generations로 이동
    """
    model = state.subgraphs.createPolicyActionsByFunctionModel
    
    if not model.parallel_worker_results:
        return state
    
    try:
        # 모든 성공한 Policy 액션들의 액션 수집
        all_actions = []
        successful_policies = []
        failed_policies = []
        es_value = {
            "elements": state.outputs.esValue.elements,
            "relations": state.outputs.esValue.relations
        }
        
        model.parallel_worker_results.sort(key=lambda x: x.worker_index)
        for policy_result in model.parallel_worker_results:
            bc_name = policy_result.target_bounded_context_name
            
            if policy_result.generation_complete and policy_result.extractedPolicies:
                successful_policies.append(policy_result)
                
                created_actions_result = _to_policy_creation_actions(
                    policy_result.extractedPolicies, 
                    EsAliasTransManager(es_value),
                    es_value,
                    bc_name,
                    model.created_policy_relations
                )
                model.created_policy_relations = created_actions_result["processed_policy_relations"]
                if not created_actions_result["created_actions"]:
                    continue

                created_actions = [ActionModel(**action) for action in created_actions_result["created_actions"] if action]

                try:
                    EsTraceUtil.convert_refs_to_indexes(
                        created_actions, policy_result.original_description,
                        policy_result.requirement_index_mapping, state, "[POLICY_ACTIONS_SUBGRAPH]",
                        full_requirements_text=state.inputs.requirements
                    )
                except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
                    LogUtil.add_exception_object_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Failed to convert source references for '{bc_name}'", e)

                all_actions.extend(created_actions)
    
            else:
                failed_policies.append(policy_result)
                LogUtil.add_error_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Policy bounded context '{bc_name}' failed or has no actions")
        
        if all_actions:   
            updated_es_value = EsActionsUtil.apply_actions(
                state.outputs.esValue.model_dump(),
                all_actions,
                state.inputs.ids.uid,
                state.inputs.ids.projectId
            )
            
            state.outputs.esValue = updated_es_value
            
        # 성공한 Policy들을 완료 목록으로 이동 (변수 정리)
        for policy in successful_policies:
            # 메모리 절약을 위한 변수 정리
            policy.description = ""
            policy.original_description = ""
            policy.summarized_es_value = {}
            policy.extractedPolicies = []

            model.completed_generations.append(policy)
        
        # 실패한 Policy들도 완료 목록으로 이동 (재시도는 하지 않음)
        for policy in failed_policies:
            policy.description = ""
            policy.original_description = ""
            policy.summarized_es_value = {}
            policy.extractedPolicies = []
            
            model.completed_generations.append(policy)
        
        # 배치 처리 완료 정리
        model.current_batch = []
        model.parallel_worker_results = []
        
        successful_count = len(successful_policies)
        failed_count = len(failed_policies)
        total_completed = len(model.completed_generations)
        
        LogUtil.add_info_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Result collection completed. Batch - Successful: {successful_count}, Failed: {failed_count}. Total completed: {total_completed}")
        
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during result collection and application", e)
        model.is_failed = True
    
    return state

def complete_processing(state: State) -> State:
    """
    Policy 액션 생성 프로세스 완료
    """
    model = state.subgraphs.createPolicyActionsByFunctionModel
    try:
        if model.end_time:
            LogUtil.add_info_log(
                state,
                "[POLICY_ACTIONS_SUBGRAPH] Completion already recorded; skipping duplicate completion handling"
            )
            return state

        state.outputs.lastCompletedRootGraphNode = RESUME_NODES.ROOT_GRAPH.CREATE_POLICY_ACTIONS
        state.outputs.lastCompletedSubGraphNode = RESUME_NODES.CREATE_POLICY_ACTIONS.COMPLETE
        state.outputs.currentProgressCount = state.outputs.currentProgressCount + 1
        JobUtil.update_job_to_firebase_fire_and_forget(state)
        
        completed_count = len(model.completed_generations)
        failed = model.is_failed
        
        if failed:
            LogUtil.add_error_log(state, f"[POLICY_ACTIONS_SUBGRAPH] Policy actions generation process completed with failures. Successfully processed: {completed_count} bounded context tasks")
        
        if not failed:
            model.completed_generations = []
            model.pending_generations = []
        
        model.end_time = time.time()
        model.total_seconds = model.end_time - model.start_time
        model.is_processing = False

    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during policy actions generation process completion", e)
        model.is_failed = True

    return state

def decide_next_step(state: State) -> str:
    """
    다음 실행할 단계 결정 (배치 처리 방식)
    """
    try:
        model = state.subgraphs.createPolicyActionsByFunctionModel

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
    
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LogUtil.add_exception_object_log(state, "[POLICY_ACTIONS_SUBGRAPH] Failed during decide_next_step", e)
        state.subgraphs.createPolicyActionsByFunctionModel.is_failed = True
        return "complete"

def create_policy_actions_by_function_subgraph() -> Callable:
    """
    Policy 액션 생성 서브그래프 생성 (병렬 처리 지원)
    """
    # 서브그래프 정의
    subgraph = StateGraph(State)
    
    # 새로운 병렬 처리 노드들 추가
    subgraph.add_node("prepare", prepare_policy_actions_generation)
    subgraph.add_node("select_batch", select_batch_policy_actions)
    subgraph.add_node("execute_parallel", execute_parallel_workers)
    subgraph.add_node("collect_results", collect_and_apply_results)
    subgraph.add_node("complete", complete_processing)
    
    # 엣지 추가 (새로운 병렬 처리 플로우)
    subgraph.add_conditional_edges(START, resume_from_create_policy_actions, {
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

def _to_policy_creation_actions(policies: List[Dict[str, Any]], 
                                es_alias_trans_manager: Any,
                                es_value: Dict[str, Any],
                                sourceBoundedContextName: str,
                                created_policy_relations: List[str]) -> List[Dict[str, Any]]:
    """
    AI가 생성한 정책 정의를 새로운 Policy 생성 액션으로 변환합니다.
    - 중복 정책 생성을 방지하는 로직을 포함합니다.
    """
    created_actions = []
    processed_policy_relations = created_policy_relations.copy()
    
    for policy in policies:
        from_event_id_alias = policy.get("fromEventId", "")
        to_event_ids_aliases = policy.get("toEventIds", [])
        if not from_event_id_alias or not to_event_ids_aliases:
            continue

        from_event_uuid = es_alias_trans_manager.alias_to_uuid_dic.get(from_event_id_alias)
        to_event_uuids = [es_alias_trans_manager.alias_to_uuid_dic.get(alias) for alias in to_event_ids_aliases]
        to_event_uuids = [uuid for uuid in to_event_uuids if uuid]
        if not from_event_uuid or not to_event_uuids:
            continue

        element = es_value["elements"].get(from_event_uuid)
        if not element or "aggregate" not in element or "id" not in element["aggregate"]:
            continue
        from_aggregate_id = element["aggregate"]["id"]

        filtered_to_event_uuids = []
        for to_event_uuid in to_event_uuids:
            element = es_value["elements"].get(to_event_uuid)
            if not element or "aggregate" not in element or "id" not in element["aggregate"]:
                continue
            
            to_aggregate_id = element["aggregate"]["id"]
            if not to_aggregate_id or to_aggregate_id == from_aggregate_id:
                continue

            check_key = "-".join(sorted([from_event_uuid, to_event_uuid]))
            if check_key in processed_policy_relations:
                continue

            processed_policy_relations.append(check_key)
            filtered_to_event_uuids.append(to_event_uuid)
        if not filtered_to_event_uuids:
            continue

        source_bounded_context_id = ""
        for element in es_value["elements"].values():
            if element.get("name") == sourceBoundedContextName:
                source_bounded_context_id = element.get("id")
                break

        policy_name = policy.get("name")
        policy_id = f"pol-{CaseConvertUtil.camel_case(policy_name)}"

        created_actions.append({
            "objectType": "Policy",
            "type": "create",
            "ids": {
                "policyId": policy_id
            },
            "args": {
                "policyName": policy_name,
                "policyAlias": policy.get("alias", ""),
                "reason": policy.get("reason", ""),
                "inputEventIds": [from_event_uuid],
                "outputEventIds": filtered_to_event_uuids,
                "refs": policy.get("refs"),
                "sourceBoundedContextId": source_bounded_context_id
            }
        })

    return {
        "created_actions": created_actions,
        "processed_policy_relations": processed_policy_relations
    }
