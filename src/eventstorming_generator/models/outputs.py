from datetime import datetime
from pydantic import ConfigDict, Field
from typing import Any, Dict, List, Optional

from .base import BaseModelWithItem

class LogModel(BaseModelWithItem):
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    level: str = ""
    message: str = ""
    model_config = ConfigDict(extra="allow")

class EsValueModel(BaseModelWithItem):
    elements: Dict[str, Any] = Field(default_factory=dict)
    relations: Dict[str, Any] = Field(default_factory=dict)
    model_config = ConfigDict(extra="allow")

class OutputsModel(BaseModelWithItem):
    esValue: EsValueModel = EsValueModel()
    isCompleted: bool = False
    isFailed: bool = False
    logs: List[LogModel] = Field(default_factory=list)
    totalProgressCount: int = 0
    currentProgressCount: int = 0
    lastCompletedRootGraphNode: Optional[str] = None
    lastCompletedSubGraphNode: Optional[str] = None
    # 생성에 실패했거나 결과가 비어 최종 반영되지 못한 대상 목록.
    # 예: ["구독 관리 / 구독 (commands)"]
    # 잡 전체는 정상 종료되므로, 이 값이 없으면 사용자는 특정 Aggregate 만
    # 하위 요소가 통째로 비어있는 것을 캔버스에서 눈으로 찾아내야 했다.
    incompleteTargets: List[str] = Field(default_factory=list)