from typing import Dict, Any, List, Optional
from pydantic import Field

from ..base import BaseModelWithItem
from ..action_model import ActionModel
from ..subgraphs.create_element_names_by_draft_model import ExtractedElementNameDetail
from ...types import RequirementIndexMapping

class CommandActionGenerationState(BaseModelWithItem):
    """단일 Aggregate에 대한 Command 액션 생성 처리 상태"""
    target_bounded_context_name: str = Field(default_factory=str)
    target_aggregate_name: str = Field(default_factory=str)
    description: str = ""
    original_description: str = ""
    extracted_element_names: ExtractedElementNameDetail = Field(default_factory=ExtractedElementNameDetail)
    requirement_index_mapping: Optional[RequirementIndexMapping] = None
    worker_index: int = 0

    summarized_es_value: Dict[str, Any] = Field(default_factory=dict)
    created_actions: List[ActionModel] = Field(default_factory=list)
    # 필수 요소 누락으로 재시도할 때 직전 시도의 액션을 보관.
    # 이후 시도가 예외로 끝나면 이 값이라도 반영해 Aggregate 가 통째로 비는 것을 막는다.
    # created_actions 와 분리하지 않으면 재시도 판정(액션이 있으면 후처리로 이동)이 깨진다.
    fallback_actions: List[ActionModel] = Field(default_factory=list)

    retry_count: int = 0
    generation_complete: bool = False
    is_failed: bool = False

class CreateCommandActionsByFunctionModel(BaseModelWithItem):
    """Command 액션 생성 관련 상태 관리 모델"""
    completed_generations: List[CommandActionGenerationState] = Field(default_factory=list)
    pending_generations: List[CommandActionGenerationState] = Field(default_factory=list)
    
    worker_generations: Dict[str, CommandActionGenerationState] = Field(default_factory=dict)
    current_batch: List[CommandActionGenerationState] = Field(default_factory=list)
    parallel_worker_results: List[CommandActionGenerationState] = Field(default_factory=list)
    
    is_processing: bool = False
    all_complete: bool = False
    
    max_retry_count: int = 3
    is_failed: bool = False

    total_seconds: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0