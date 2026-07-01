from typing import Dict, Any, Optional
import copy

from .database_system import DatabaseSystem

class MemoryDBSystem(DatabaseSystem):
    """
    테스트용 인메모리 Mock 데이터베이스 시스템입니다.
    실제 DB 호출 없이 로직을 테스트할 수 있습니다.
    """
    def __init__(self):
        self._data: Dict[str, Any] = {}

    def set_data(self, path: str, data: Dict[str, Any]) -> bool:
        path_parts = path.split('/')
        current_level = self._data
        for part in path_parts[:-1]:
            current_level = current_level.setdefault(part, {})
        current_level[path_parts[-1]] = copy.deepcopy(data)
        return True

    def get_data(self, path: str) -> Optional[Dict[str, Any]]:
        path_parts = path.split('/')
        current_level = self._data
        try:
            for part in path_parts:
                current_level = current_level[part]
            return copy.deepcopy(current_level)
        except (KeyError, TypeError):
            return None

    def update_data(self, path: str, data: Dict[str, Any]) -> bool:
        existing_data = self.get_data(path)
        if isinstance(existing_data, dict):
            existing_data.update(data)
            self.set_data(path, existing_data)
        else:
            self.set_data(path, data)
        return True

    def delete_data(self, path: str) -> bool:
        path_parts = path.split('/')
        current_level = self._data
        try:
            for part in path_parts[:-1]:
                current_level = current_level[part]
            if path_parts[-1] in current_level:
                del current_level[path_parts[-1]]
                return True
        except (KeyError, TypeError):
            pass
        return False

    def conditional_update_data(self, path: str, data_to_update: Dict[str, Any], previous_data: Dict[str, Any]) -> bool:
        """
        두 데이터를 비교하여 변경된 부분만 효율적으로 업데이트
        
        Args:
            path (str): 데이터베이스 기본 경로
            data_to_update (Dict[str, Any]): 업데이트할 새로운 데이터
            previous_data (Dict[str, Any]): 기존 데이터
            
        Returns:
            bool: 성공 여부
        """
        try:
            # 데이터 차이점 찾기
            updates = self._find_data_differences(data_to_update, previous_data)
            
            # 변경사항이 없으면 업데이트하지 않음
            if not updates:
                return True
            
            # 각 업데이트 경로에 대해 적용
            for update_path, value in updates.items():
                full_path = f"{path}/{update_path}" if path else update_path
                
                if value is None:
                    # 삭제 처리
                    self.delete_data(full_path)
                else:
                    # 업데이트 처리 - 경로의 상위 레벨을 확보
                    path_parts = full_path.split('/')
                    current_level = self._data
                    
                    # 마지막 키를 제외한 경로를 탐색하며 딕셔너리 생성
                    for part in path_parts[:-1]:
                        if part not in current_level:
                            current_level[part] = {}
                        current_level = current_level[part]
                    
                    # 최종 값 설정
                    current_level[path_parts[-1]] = copy.deepcopy(value)
            
            return True
        except Exception:
            return False

    def clear(self):
        """테스트 간 데이터베이스 상태를 초기화합니다."""
        self._data = {}
    
    def get_all_data(self) -> Dict[str, Any]:
        return copy.deepcopy(self._data)