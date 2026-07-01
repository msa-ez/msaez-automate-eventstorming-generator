"""
단일 UI 컴포넌트 처리를 위한 워커 서브그래프

이 모듈은 하나의 UI 컴포넌트에 대해 preprocess -> generate -> postprocess -> validate
순으로 처리하는 워커 서브그래프를 제공합니다.

메인 오케스트레이터에서 병렬로 여러 워커를 실행하는 데 사용됩니다.
"""

from typing import Optional
from contextvars import ContextVar
from langgraph.graph import StateGraph, START

from ...models import CreateUiComponentsGenerationState, ActionModel, State, CreateCommandWireFrameOutput, CreateReadModelWireFrameOutput
from ...utils import JsonUtil, LogUtil
from ...generators import CreateCommandWireFrame, CreateReadModelWireFrame
from ...config import Config

# 스레드로부터 안전한 컨텍스트 변수 생성
ui_component_worker_id_context = ContextVar('worker_id', default=None)

def get_current_generation(state: State) -> Optional[CreateUiComponentsGenerationState]:
    """
    현재 워커의 ID를 사용하여 해당하는 generation state를 반환합니다.
    메모리 최적화를 위해 worker_generations 딕셔너리를 사용합니다.
    """
    model = state.subgraphs.createUiComponentsModel
    worker_id = ui_component_worker_id_context.get()  # 공유 상태가 아닌 컨텍스트 변수에서 ID를 가져옴
    
    if not worker_id:
        LogUtil.add_error_log(state, "[UI_WORKER] Current worker ID not found in state")
        return None
    
    if worker_id not in model.worker_generations:
        LogUtil.add_error_log(state, f"[UI_WORKER] Worker generation not found for worker_id: {worker_id}")
        return None
    
    return model.worker_generations[worker_id]

def worker_preprocess_ui_component(state: State) -> State:
    """
    단일 UI 컴포넌트 전처리 (워커 전용)
    - state.subgraphs.createUiComponentsModel.worker_generations에서 해당 워커의 UI를 가져와 처리
    - Command/ReadModel 타입 분석 및 AI 입력 데이터 구성
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[UI_WORKER] No current generation found in worker preprocess")
        return state
        
    ui_name = current_gen.target_ui_component.get("name", "Unknown")
    
    try:
        target_ui_component = current_gen.target_ui_component
        
        if target_ui_component.get("command"):
            command_id = target_ui_component.get("command").get("id")
            command_element = state.outputs.esValue.elements.get(command_id)
            
            if command_element:
                fields = []
                for field in command_element.get("fieldDescriptors", []):
                    fields.append({
                        "name": field.get("name"),
                        "type": field.get("className") or "String",
                        "required": field.get("isKey") or False
                    })
                
                api_method = command_element.get("controllerInfo", {}).get("method") or "POST"
                api_path = command_element.get("controllerInfo", {}).get("apiPath") or "N/A"
                api = f"{api_method} {api_path}"
                
                current_gen.ai_request_type = "Command"
                current_gen.ai_input_data = {
                    "commandName": command_element.get("name"),
                    "commandDisplayName": command_element.get("displayName"),
                    "fields": fields,
                    "api": api
                }
                
        elif target_ui_component.get("readModel"):
            read_model_id = target_ui_component.get("readModel").get("id")
            read_model_element = state.outputs.esValue.elements.get(read_model_id)
            
            if read_model_element:
                view_query_parameters = []
                for query_param in read_model_element.get("queryParameters", []):
                    view_query_parameters.append({
                        "name": query_param.get("name"),
                        "type": query_param.get("className") or "String"
                    })
                
                aggregate_id = read_model_element.get("aggregate", {}).get("id")
                aggregate_element = state.outputs.esValue.elements.get(aggregate_id)
                aggregate_fields = []
                if aggregate_element:
                    aggregate_root = aggregate_element.get("aggregateRoot", {})
                    if aggregate_root:
                        for field in aggregate_root.get("fieldDescriptors", []):
                            aggregate_fields.append({
                                "name": field.get("name"),
                                "type": field.get("className") or "String"
                            })

                current_gen.ai_request_type = "ReadModel"
                current_gen.ai_input_data = {
                    "viewName": read_model_element.get("name"),
                    "viewDisplayName": read_model_element.get("displayName"),
                    "aggregateFields": aggregate_fields,
                    "viewQueryParameters": view_query_parameters
                }
        
        else:
            LogUtil.add_error_log(state, f"[UI_WORKER] UI component '{ui_name}' has no Command or ReadModel attached")
            current_gen.is_failed = True
            return state
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[UI_WORKER] Preprocessing failed for UI component '{ui_name}'", e)
        current_gen.is_failed = True
    
    return state

def worker_generate_ui_component(state: State) -> State:
    """
    단일 UI 컴포넌트 Wireframe 생성 (워커 전용)
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[UI_WORKER] No current generation found in worker generate")
        return state
        
    ui_name = current_gen.target_ui_component.get("name", "Unknown")
    
    try:

        model_name = Config.get_ai_model_light()
        
        generator = None
        generated_html = ""
        if current_gen.ai_request_type == "Command":
            generator = CreateCommandWireFrame(
                model_name=model_name,
                client={
                    "inputs": current_gen.ai_input_data,
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
            generator_result: CreateCommandWireFrameOutput = generator_output["result"]
            generated_html = generator_result.html

        elif current_gen.ai_request_type == "ReadModel":
            generator = CreateReadModelWireFrame(
                model_name=model_name,
                client={
                    "inputs": current_gen.ai_input_data,
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
            generator_result: CreateReadModelWireFrameOutput = generator_output["result"]
            generated_html = generator_result.html

        else:
            LogUtil.add_error_log(state, f"[UI_WORKER] Invalid AI request type '{current_gen.ai_request_type}' for UI component '{ui_name}'")
            current_gen.is_failed = True
            return state
        
        ui_update_action = ActionModel(
            objectType="UI",
            type="update",
            ids={"uiId": current_gen.target_ui_component["id"]},
            args={"runTimeTemplateHtml": generated_html}
        )
        
        current_gen.ui_replace_actions = [ui_update_action]
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[UI_WORKER] Failed to generate wireframe for UI component '{ui_name}'", e)
        if current_gen:
            current_gen.retry_count += 1

    return state

def worker_postprocess_ui_component(state: State) -> State:
    """
    단일 UI 컴포넌트 후처리 (워커 전용)
    - 워커에서는 ES 모델 업데이트 없이 액션만 저장하고 완료 표시
    - 실제 ES 업데이트는 메인 오케스트레이터에서 일괄 처리
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[UI_WORKER] No current generation found in worker postprocess")
        return state
        
    ui_name = current_gen.target_ui_component.get("name", "Unknown")
    
    try:
        # 생성된 UI replace actions가 없으면 실패로 처리
        if not current_gen.ui_replace_actions:
            current_gen.retry_count += 1
            return state
        
        # 워커에서는 ES 업데이트를 하지 않고 완료 표시만 함
        current_gen.generation_complete = True
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[UI_WORKER] Postprocessing failed for UI component '{ui_name}'", e)
        if current_gen:
            current_gen.retry_count += 1
            current_gen.ui_replace_actions = []

    return state

def worker_validate_ui_component(state: State) -> State:
    """
    단일 UI 컴포넌트 검증 (워커 전용)
    - 생성 완료 및 재시도 횟수 확인
    """
    current_gen = get_current_generation(state)
    if not current_gen:
        LogUtil.add_error_log(state, "[UI_WORKER] No current generation found in worker validate")
        return state
        
    ui_name = current_gen.target_ui_component.get("name", "Unknown")
    
    try:
        # 최대 재시도 횟수 초과 시 실패로 처리
        if current_gen.retry_count > state.subgraphs.createUiComponentsModel.max_retry_count:
            LogUtil.add_error_log(state, f"[UI_WORKER] Maximum retry count exceeded for UI component '{ui_name}' (retries: {current_gen.retry_count})")
            current_gen.is_failed = True
        
    except Exception as e:
        LogUtil.add_exception_object_log(state, f"[UI_WORKER] Validation failed for UI component '{ui_name}'", e)
        current_gen.is_failed = True

    return state

def worker_decide_next_step(state: State) -> str:
    """
    워커 내에서 다음 단계 결정
    """
    try:
        current_gen = get_current_generation(state)
        
        if not current_gen:
            LogUtil.add_error_log(state, "[UI_WORKER] No current generation found in decide_next_step")
            return "complete"

        # 실패 혹은 최대 재시도 횟수 초과 시 완료
        if current_gen.is_failed or current_gen.retry_count > state.subgraphs.createUiComponentsModel.max_retry_count:
            return "complete"
        
        # 현재 작업이 완료되었으면 완료
        if current_gen.generation_complete:
            return "complete"
        
        # 전처리로 인한 요약 정보가 없을 경우, 전처리 단계로 이동
        if not current_gen.ai_input_data or not current_gen.ai_request_type:
            return "preprocess"
        
        # 기본적으로 생성 실행 단계로 이동
        if not current_gen.ui_replace_actions:
            return "generate"
        
        # 생성된 UI Components가 있으면 후처리 단계로 이동
        return "postprocess"
    
    except Exception as e:
        LogUtil.add_exception_object_log(state, "[UI_WORKER] Failed during worker_decide_next_step", e)
        return "complete"

def create_ui_component_worker_subgraph():
    """
    단일 UI 컴포넌트 처리를 위한 워커 서브그래프 생성
    
    Returns:
        Callable: 컴파일된 워커 서브그래프 실행 함수
    """
    # 워커 서브그래프 정의
    worker_graph = StateGraph(State)
    
    # 노드 추가
    worker_graph.add_node("preprocess", worker_preprocess_ui_component)
    worker_graph.add_node("generate", worker_generate_ui_component) 
    worker_graph.add_node("postprocess", worker_postprocess_ui_component)
    worker_graph.add_node("validate", worker_validate_ui_component)
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
            state: current_generation에 처리할 UI가 설정된 State
            
        Returns:
            State: 처리 완료된 current_generation을 포함한 State
        """
        try:
            result = State(**compiled_worker.invoke(state, {"recursion_limit": 2147483647}))
            return result
        except Exception as e:
            LogUtil.add_exception_object_log(state, "[UI_WORKER] Worker execution failed", e)
            current_gen = get_current_generation(state)
            if current_gen:
                current_gen.is_failed = True
            return state
    
    return run_worker
