import os
import asyncio
import concurrent.futures
import json
import time
from typing import Dict, Any, Optional, Callable, Callable
from functools import partial
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ...utils.logging_util import LoggingUtil
from .database_system import DatabaseSystem


class AceBaseSystem(DatabaseSystem):
    """AceBase Database 시스템 구현"""
    
    _instance: Optional['AceBaseSystem'] = None
    _initialized: bool = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, host: str = None, port: int = None, dbname: str = None, 
                 https: bool = False, username: str = None, password: str = None):
        """
        AceBase 시스템 초기화 (싱글톤)
        
        Args:
            host (str): AceBase 서버 호스트
            port (int): AceBase 서버 포트
            dbname (str): 데이터베이스 이름
            https (bool): HTTPS 사용 여부
            username (str): 인증 사용자명
            password (str): 인증 비밀번호
        """
        # 이미 초기화된 경우 중복 초기화 방지
        if self._initialized:
            return
        
        if host is None or port is None or dbname is None:
            raise ValueError("host, port, dbname은 필수 매개변수입니다.")
        
        self.host = host
        self.port = port
        self.dbname = dbname
        self.https = https
        self.protocol = "https" if https else "http"
        self.base_url = f"{self.protocol}://{self.host}:{self.port}"
        # AceBase HTTP API는 /data/{dbname}/{path} 형식 사용
        self.api_url = f"{self.base_url}/data/{self.dbname}"
        
        # 세션 설정 (재시도 로직 포함)
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.access_token: Optional[str] = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        
        # 자격증명 보관 (토큰 만료/403 발생 시 재인증용)
        self._auth_username = username if (username and password) else None
        self._auth_password = password if (username and password) else None

        # 인증 처리 (선택적 - AceBase 서버가 인증 없이도 동작 가능한 경우 존재)
        if self._auth_username and self._auth_password:
            self._authenticate(self._auth_username, self._auth_password)
            if not self.access_token:
                # 자격증명을 명시적으로 제공했는데도 토큰을 받지 못하면 조용히 넘기지 않음.
                # 이전 동작은 인증 실패를 INFO 로그로만 남기고 토큰 없이 진행하여, 이후 모든
                # 데이터 호출이 403 으로 실패하는데 사용자에게는 원인이 드러나지 않았음.
                raise RuntimeError(
                    f"AceBase 인증 실패: {username}@{self.base_url} (dbname={self.dbname}). "
                    f"ACEBASE_USERNAME / ACEBASE_PASSWORD 및 서버 주소를 확인하세요."
                )
        else:
            # 인증 정보가 제공되지 않은 경우 (정상 동작)
            LoggingUtil.info("acebase_system", "AceBase 인증 정보 없음: 인증 없이 진행")
            self.access_token = None

        self._initialized = True

    def _authenticate(self, username: str, password: str) -> None:
        """AceBase 인증.

        msa-ez/acebase 이미지(``ghcr.io/msa-ez/acebase``)는 OAuth2 라우터로
        마운트되어 있어 사인인 경로가 ``/oauth2/{dbname}/signin`` 이다 (실측 확인).
        표준 acebase-server 의 ``/auth/{dbname}/signin`` 등은 fallback 으로 둔다.
        일부 버전은 바디에 ``method`` 필드를 요구하므로 ``"internal"`` 을 함께 보낸다.
        """
        auth_endpoints = [
            f"{self.base_url}/oauth2/{self.dbname}/signin",  # msa-ez/acebase (실측 동작)
            f"{self.base_url}/auth/{self.dbname}/signin",    # acebase-server 표준
            f"{self.base_url}/auth/signin",                   # 구버전 호환
            f"{self.api_url}/auth/signin",                    # 구버전 호환
            f"{self.base_url}/api/auth/signin",               # 구버전 호환
        ]
        payload = {
            "method": "internal",
            "username": username,
            "password": password,
        }

        last_error: Optional[str] = None
        for auth_url in auth_endpoints:
            try:
                response = self.session.post(auth_url, json=payload, timeout=5)
            except requests.exceptions.RequestException as e:
                last_error = f"{auth_url}: {e}"
                continue

            if response.status_code == 200:
                try:
                    result = response.json()
                except ValueError:
                    last_error = f"{auth_url}: 200 OK 이지만 JSON 파싱 실패"
                    continue
                token = result.get("access_token") or result.get("accessToken")
                if token:
                    self.access_token = token
                    LoggingUtil.info(
                        "acebase_system",
                        f"AceBase 인증 성공: {username} via {auth_url}",
                    )
                    return
                last_error = f"{auth_url}: 200 OK 이지만 토큰 미포함 ({result!r})"
            else:
                # 본문에 사유가 들어있는 경우가 많아 함께 남긴다
                body_preview = (response.text or "")[:200]
                last_error = f"{auth_url}: HTTP {response.status_code} {body_preview}"

        LoggingUtil.warning(
            "acebase_system",
            f"AceBase 인증 시도 실패: {last_error}",
        )
        self.access_token = None
    
    @classmethod
    def initialize(cls, host: str = None, port: int = None, dbname: str = None,
                   https: bool = False, username: str = None, password: str = None) -> 'AceBaseSystem':
        """
        싱글톤 인스턴스 초기화
        
        Args:
            host (str): AceBase 서버 호스트
            port (int): AceBase 서버 포트
            dbname (str): 데이터베이스 이름
            https (bool): HTTPS 사용 여부
            username (str): 인증 사용자명
            password (str): 인증 비밀번호
            
        Returns:
            AceBaseSystem: 초기화된 싱글톤 인스턴스
        """
        if cls._instance is None or not cls._instance._initialized:
            cls._instance = cls(host, port, dbname, https, username, password)
        return cls._instance
    
    @classmethod
    def instance(cls) -> 'AceBaseSystem':
        """
        싱글톤 인스턴스 반환
        
        Returns:
            AceBaseSystem: 초기화된 싱글톤 인스턴스
            
        Raises:
            RuntimeError: 인스턴스가 초기화되지 않은 경우
        """
        if cls._instance is None or not cls._instance._initialized:
            raise RuntimeError("AceBaseSystem 초기화되지 않았습니다. 먼저 AceBaseSystem.initialize()를 호출하세요.")
        return cls._instance
    
    def _get_path_url(self, path: str) -> str:
        """경로를 AceBase API URL로 변환"""
        # 경로의 시작 슬래시 제거
        clean_path = path.lstrip('/')
        # AceBase HTTP API는 /data/{dbname}/{path} 형식 사용
        return f"{self.api_url}/{clean_path}"
    
    def _get_headers(self) -> Dict[str, str]:
        """요청 헤더 생성"""
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers
    
    def _execute_with_error_handling(self, operation_name: str, operation_func: Callable, *args, **kwargs) -> Any:
        """
        에러 처리가 포함된 공통 실행 래퍼
        
        Args:
            operation_name (str): 작업 이름 (에러 메시지용)
            operation_func (Callable): 실행할 함수
            *args, **kwargs: 함수에 전달할 인수들
            
        Returns:
            Any: 실행 결과 또는 실패 시 기본값
        """
        try:
            return operation_func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                # 404는 정상 케이스 (경로가 아직 없음) - 에러 로그 없이 None 반환
                return None
            # 401/403: 토큰 만료/누락 가능성 → 1회 재인증 후 동일 작업 재시도
            if status in (401, 403) and self._auth_username and self._auth_password:
                LoggingUtil.info(
                    "acebase_system",
                    f"{operation_name} 중 인증 오류({status}) 감지, 재인증 후 재시도",
                )
                self.access_token = None
                self._authenticate(self._auth_username, self._auth_password)
                if self.access_token:
                    try:
                        return operation_func(*args, **kwargs)
                    except Exception as retry_err:
                        LoggingUtil.exception(
                            "acebase_system",
                            f"{operation_name} 재인증 후 재시도 실패",
                            retry_err,
                        )
                        return False if operation_name.endswith(('업로드', '업데이트', '삭제', '시작', '중단')) else None
            LoggingUtil.exception("acebase_system", f"{operation_name} 실패", e)
            return False if operation_name.endswith(('업로드', '업데이트', '삭제', '시작', '중단')) else None
        except Exception as e:
            LoggingUtil.exception("acebase_system", f"{operation_name} 실패", e)
            return False if operation_name.endswith(('업로드', '업데이트', '삭제', '시작', '중단')) else None
    
    async def _execute_async_with_error_handling(self, operation_name: str, sync_func: Callable, *args, **kwargs) -> Any:
        """
        비동기 실행을 위한 공통 래퍼
        
        Args:
            operation_name (str): 작업 이름
            sync_func (Callable): 동기 함수
            *args, **kwargs: 함수에 전달할 인수들
            
        Returns:
            Any: 실행 결과
        """
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                self._executor,
                partial(sync_func, *args, **kwargs)
            )
            return result
        except Exception as e:
            LoggingUtil.exception("acebase_system", f"비동기 {operation_name} 실패", e)
            return False if operation_name.endswith(('업로드', '업데이트', '삭제', '시작', '중단')) else None
    
    # =============================================================================
    # 데이터 설정 메서드들
    # =============================================================================
    
    def set_data(self, path: str, data: Dict[str, Any]) -> bool:
        """특정 경로에 딕셔너리 데이터를 업로드"""
        def _set_operation():
            url = self._get_path_url(path)
            sanitized_data = self.sanitize_data_for_storage(data)
            # AceBase는 {"val": {...}} 형식을 요구함
            payload = {"val": sanitized_data}
            response = self.session.put(
                url,
                json=payload,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True
        
        return self._execute_with_error_handling("데이터 업로드", _set_operation)
    
    # =============================================================================
    # 데이터 업데이트 메서드들
    # =============================================================================
    
    def update_data(self, path: str, data: Any) -> bool:
        """특정 경로의 데이터를 부분 업데이트 (딕셔너리 또는 단일 값 모두 지원)"""
        def _update_operation():
            url = self._get_path_url(path)
            
            # 단일 값(primitive type)인 경우 딕셔너리로 변환
            if not isinstance(data, dict):
                # 단일 값을 직접 전달 (AceBase는 {"val": value} 형식 사용)
                payload = {"val": data}
            else:
                # 딕셔너리인 경우 sanitize 후 전달
                sanitized_data = self.sanitize_data_for_storage(data)
                payload = {"val": sanitized_data}
            
            response = self.session.post(
                url,
                json=payload,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True
        
        return self._execute_with_error_handling("데이터 업데이트", _update_operation)
    
    # =============================================================================
    # 조건부 업데이트 메서드들
    # =============================================================================
    
    def conditional_update_data(self, path: str, data_to_update: Dict[str, Any], previous_data: Dict[str, Any]) -> bool:
        """두 데이터를 비교하여 변경된 부분만 효율적으로 업데이트"""
        # AceBase는 Firebase와 달리 부분 업데이트를 직접 지원하므로
        # 차이점을 찾아서 업데이트
        updates = self._find_data_differences(
            self.sanitize_data_for_storage(data_to_update),
            self.sanitize_data_for_storage(previous_data)
        )
        
        if not updates:
            return True
        
        # 각 업데이트 경로에 대해 개별적으로 업데이트
        for update_path, value in updates.items():
            full_path = f"{path}/{update_path}" if path else update_path
            if value is None:
                # 삭제
                self.delete_data(full_path)
            else:
                # 업데이트: full_path가 이미 최종 필드 경로를 포함하므로
                # update_data는 {"val": {...}} 형식으로 저장하는데,
                # full_path가 이미 필드 경로이므로 value를 직접 저장해야 함
                # 단순 값(문자열, 숫자 등)인 경우와 딕셔너리인 경우를 구분
                if isinstance(value, dict):
                    # 딕셔너리인 경우 그대로 전달 (이미 올바른 형식)
                    self.update_data(full_path, value)
                else:
                    # 단순 값인 경우: full_path가 이미 필드 경로이므로
                    # set_data를 사용하여 직접 값을 저장
                    # sanitize_data_for_storage는 딕셔너리만 받으므로, 단순 값은 process_value를 직접 사용
                    def process_simple_value(val):
                        if val is None:
                            return "@"  # null → 빈 문자열
                        elif isinstance(val, list) and len(val) == 0:
                            return ["@"]  # 빈 배열 → 마커가 포함된 배열
                        elif isinstance(val, dict) and len(val) == 0:
                            return {"@": True}  # 빈 객체 → 마커 객체
                        elif isinstance(val, dict):
                            return {k: process_simple_value(v) for k, v in val.items()}
                        elif isinstance(val, list):
                            return [process_simple_value(item) for item in val]
                        else:
                            return val
                    
                    sanitized_value = process_simple_value(value)
                    url = self._get_path_url(full_path)
                    payload = {"val": sanitized_value}
                    try:
                        response = self.session.put(
                            url,
                            json=payload,
                            headers=self._get_headers(),
                            timeout=30
                        )
                        response.raise_for_status()
                    except Exception as e:
                        LoggingUtil.exception("acebase_system", f"필드 업데이트 실패: {full_path}", e)
                        return False
        
        return True
    
    # =============================================================================
    # 데이터 조회 메서드들
    # =============================================================================
    
    def get_data(self, path: str) -> Optional[Dict[str, Any]]:
        """특정 경로에서 데이터를 딕셔너리 형태로 조회"""
        def _get_operation():
            url = self._get_path_url(path)
            try:
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=30
                )
                # 404는 데이터가 없는 것으로 처리 (에러가 아님)
                if response.status_code == 404:
                    return None
                
                response.raise_for_status()
                result = response.json()
                
                # AceBase API 응답 형식: {"exists":true/false,"val":{...}}
                if not result.get("exists", False):
                    return None
                
                data = result.get("val")
                if data is None:
                    return None
                
                if isinstance(data, dict):
                    return self.restore_data_from_storage(data)
                return data
            except requests.exceptions.HTTPError as e:
                # 404는 데이터가 없는 것으로 처리
                if e.response and e.response.status_code == 404:
                    return None
                raise
        
        return self._execute_with_error_handling("데이터 조회", _get_operation)

    def get_children_data(self, path: str) -> Optional[Dict[str, Dict[str, Any]]]:
        """특정 경로의 모든 자식 노드 데이터를 조회"""
        data = self.get_data(path)
        if data is None or not isinstance(data, dict):
            return None
        return data
    
    # =============================================================================
    # 데이터 삭제 메서드들
    # =============================================================================
    
    def delete_data(self, path: str) -> bool:
        """특정 경로의 데이터 삭제"""
        def _delete_operation():
            # 경로를 부모 경로와 자식 키로 분리
            path_parts = path.rstrip('/').split('/')
            if len(path_parts) < 2:
                # 루트 경로나 단일 경로는 직접 삭제
                url = self._get_path_url(path)
                payload = {"val": None}
            else:
                # 부모 경로에서 자식만 삭제하는 방식 사용
                parent_path = '/'.join(path_parts[:-1])
                child_key = path_parts[-1]
                url = self._get_path_url(parent_path)
                # 부모 경로에서 특정 자식만 null로 설정하여 삭제
                payload = {"val": {child_key: None}}
            
            try:
                # update 방식으로 부모 경로에서 자식만 삭제
                response = self.session.post(
                    url,
                    json=payload,
                    headers=self._get_headers(),
                    timeout=30
                )
                # 404는 이미 삭제되었거나 존재하지 않는 것으로 처리 (에러가 아님)
                if response.status_code == 404:
                    LoggingUtil.info("acebase_system", f"데이터 삭제: 경로가 이미 존재하지 않습니다 (404): {path}")
                    return True
                response.raise_for_status()
                return True
            except requests.exceptions.HTTPError as e:
                # 404는 이미 삭제되었거나 존재하지 않는 것으로 처리
                if e.response and e.response.status_code == 404:
                    LoggingUtil.info("acebase_system", f"데이터 삭제: 경로가 이미 존재하지 않습니다 (404): {path}")
                    return True
                raise
        
        return self._execute_with_error_handling("데이터 삭제", _delete_operation)
    
    # =============================================================================
    # 데이터 정제 메서드들
    # =============================================================================
    
    def sanitize_data_for_storage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Storage 업로드를 위해 데이터 정제 (AceBase는 Firebase와 동일한 방식 사용)"""
        def process_value(value):
            if value is None:
                return "@"  # null → 빈 문자열
            elif isinstance(value, list) and len(value) == 0:
                return ["@"]  # 빈 배열 → 마커가 포함된 배열
            elif isinstance(value, dict) and len(value) == 0:
                return {"@": True}  # 빈 객체 → 마커 객체
            elif isinstance(value, dict):
                return {k: process_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [process_value(item) for item in value]
            else:
                return value
        
        return {k: process_value(v) for k, v in data.items()}
    
    def restore_data_from_storage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Storage에서 가져온 데이터를 원본 형태로 복원"""
        def process_value(value):
            if value == "@":
                return None  # 빈 문자열 → null
            elif isinstance(value, list) and value == ["@"]:
                return []  # 마커 → 빈 배열
            elif isinstance(value, dict) and value == {"@": True}:
                return {}  # 마커 객체 → 빈 객체
            elif isinstance(value, dict):
                return {k: process_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [process_value(item) for item in value]
            else:
                return value
        
        return {k: process_value(v) for k, v in data.items()}

    # =============================================================================
    # 데이터 감시 메서드들 (AceBase HTTP API는 실시간 watch를 지원하지 않음)
    # =============================================================================
    
    def watch_data(self, path: str, callback: Callable[[Optional[Dict[str, Any]]], None]) -> bool:
        """
        특정 경로의 데이터 변화를 감시합니다.
        
        Note: AceBase HTTP API는 실시간 watch를 지원하지 않으므로,
        주기적으로 폴링하는 방식으로 구현할 수 있습니다.
        현재는 기본 구현만 제공합니다.
        
        Args:
            path (str): 데이터베이스 경로
            callback (Callable): 데이터 변화 시 호출할 콜백 함수
            
        Returns:
            bool: 감시 시작 성공 여부
        """
        # TODO: AceBase HTTP API에서 실시간 watch 지원 시 구현
        # 현재는 Firebase만 지원하므로 경고만 출력
        LoggingUtil.warning("acebase_system", f"watch_data는 AceBase에서 아직 지원되지 않습니다: {path}")
        return False

    def unwatch_data(self, path: str) -> bool:
        """
        특정 경로의 데이터 감시를 중단합니다.
        
        Args:
            path (str): 데이터베이스 경로
            
        Returns:
            bool: 감시 중단 성공 여부
        """
        # TODO: AceBase HTTP API에서 실시간 watch 지원 시 구현
        LoggingUtil.warning("acebase_system", f"unwatch_data는 AceBase에서 아직 지원되지 않습니다: {path}")
        return False

