import asyncio
import secrets
import re
import time
import uuid
from typing import AsyncGenerator, Dict, Any, Optional

from .job_util import JobUtil
from .job_requirements_util import JobRequirementsUtil
from .a2a_session_manager import A2ASessionManager
from ...config import Config
from ...models import State, InputsModel, IdsModel
from ...constants import REQUEST_TYPES
from ...systems.database.database_factory import DatabaseFactory
from ...utils.smart_logger import SmartLogger


# 로깅 카테고리
CATEGORY = "job_request"


class JobRequestUtil:
    @staticmethod
    async def add_job_request_with_streaming(requirements: str) -> AsyncGenerator[Dict[str, Any], None]:
        """
        async generator로 작업 요청 후 로그 스트리밍
        
        1. 작업 큐에 추가
        2. A2A 세션 등록
        3. asyncio.Queue 생성하여 Firebase watch 콜백과 연결
        4. 초기 응답 yield (job_id, link)
        5. 로그 변경 시 새 로그 yield
        6. 완료(isCompleted)/실패(isFailed) 감지 시 최종 응답 yield 후 종료
        7. A2A 세션 해제 및 watch 해제
        """
        session_id = str(uuid.uuid4())
        session_manager = A2ASessionManager.instance()
        job_id: Optional[str] = None
        link: Optional[str] = None
        last_log_count = 0
        
        try:
            # 1. 작업 큐에 추가
            job_id, link = JobRequestUtil._add_job_request_by_requirements_with_id(requirements)
            
            # 2. A2A 세션 등록
            session_manager.register_session(session_id)
            SmartLogger.log("DEBUG", "A2A 세션 등록", category=CATEGORY, params={
                "session_id": session_id,
                "job_id": job_id,
            })
            
            # 3. 초기 응답 yield
            yield {
                "type": "status_update",
                "state": "working",
                "job_id": job_id,
                "link": link,
                "message": "작업이 큐에 추가되었습니다."
            }
            

            # 4. Firebase watch 콜백 설정
            watch_status = JobRequestUtil._watch_job_status(job_id)
            job_request_path = Config.get_requested_job_path(job_id)

            # 5. 데이터 변경 감시 및 스트리밍
            while True:
                try:
                    # Queue에서 데이터 대기
                    data = await asyncio.wait_for(watch_status["queue"].get(), timeout=1800.0)

                    if data.get("type") == "logs":
                        logs = data.get("data", [])
                        current_log_count = len(logs) if isinstance(logs, list) else 0
                        
                        if current_log_count > last_log_count:
                            # 새 로그만 전송
                            new_logs = logs[last_log_count:] if isinstance(logs, list) else []
                            for log in new_logs:
                                yield {
                                    "type": "log",
                                    "state": "working",
                                    "job_id": job_id,
                                    "log": {
                                        "level": log.get("level", "info"),
                                        "message": log.get("message", ""),
                                        "created_at": log.get("created_at", "")
                                    }
                                }
                            last_log_count = current_log_count
                    
                    elif data.get("type") == "is_completed":
                        is_completed = data.get("data", False)
                        if is_completed:
                            yield {
                                "type": "completed",
                                "state": "completed",
                                "job_id": job_id,
                                "link": link,
                                "message": "Event Storming generation complete."
                            }
                            break
                    
                    elif data.get("type") == "is_failed":
                        is_failed = data.get("data", False)
                        if is_failed:
                            yield {
                                "type": "failed",
                                "state": "failed",
                                "job_id": job_id,
                                "link": link,
                                "message": "An error occurred during processing."
                            }
                            break
                    
                except asyncio.TimeoutError:
                    # 타임아웃 시 현재 상태 직접 조회
                    db_system = DatabaseFactory.get_db_system()
                    current_data = db_system.get_data(job_request_path)
                    if current_data is None:
                        # Job 데이터가 삭제됨 - 완료로 간주
                        yield {
                            "type": "completed",
                            "state": "completed",
                            "job_id": job_id,
                            "link": link,
                            "message": "Event Storming generation complete."
                        }
                        break
                    
        except Exception as e:
            SmartLogger.log("ERROR", "A2A 스트리밍 오류", category=CATEGORY, params={
                "job_id": job_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
            })
            yield {
                "type": "error",
                "state": "failed",
                "job_id": job_id,
                "link": link,
                "message": f"스트리밍 오류: {str(e)}"
            }
            
        finally:
            # 7. Watch 해제 및 세션 해제
            # clear_watch_paths를 executor에서 비동기로 실행
            # (동기적 Firebase 연결 해제가 asyncio 이벤트 루프를 블록하지 않도록)
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, watch_status["clear_watch_paths"])
            except Exception as e:
                SmartLogger.log("ERROR", "Watch 해제 오류", category=CATEGORY, params={
                    "session_id": session_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                })
            
            session_manager.unregister_session(session_id)
            SmartLogger.log("DEBUG", "A2A 세션 해제", category=CATEGORY, params={
                "session_id": session_id,
            })
    

    @staticmethod
    def add_job_request_by_requirements(requirements: str) -> tuple[str, str]:
        """
        작업 요청을 추가하고 job_id와 link를 반환 (Public API)
        
        비스트리밍 방식의 A2A executor에서 사용합니다.
        
        Args:
            requirements: 이벤트 스토밍 생성 요구사항
            
        Returns:
            tuple[str, str]: (job_id, link) 튜플
        """
        return JobRequestUtil._add_job_request_by_requirements_with_id(requirements)

    @staticmethod
    def _add_job_request_by_requirements_with_id(requirements: str) -> tuple[str, str]:
        """작업 요청을 추가하고 job_id와 link를 함께 반환"""
        parsed_requirements = JobRequirementsUtil.parse_requirements(requirements)
        job_state = JobRequestUtil._make_job_state(parsed_requirements)
        job_id = job_state.inputs.jobId

        db_system = DatabaseFactory.get_db_system()
        db_system.set_data(Config.get_job_path(job_id), {
            "state": job_state.model_dump(),
            "lastUpdated": time.time()
        })
        db_system.set_data(Config.get_requested_job_path(job_id), {
            "createdAt": time.time()
        })

        link = JobRequestUtil._make_event_storming_link(job_id)
        return job_id, link
    
    @staticmethod
    def _make_job_state(requirements: str) -> State:
        job_id = JobUtil.create_job_id()
        uid = "temp-uid-" + job_id
        projectId = "temp-project-id-" + job_id

        state = State(
            inputs=InputsModel(
                requestType=REQUEST_TYPES.FROM_REQUIREMENTS,
                jobId=job_id,
                ids=IdsModel(
                    uid=uid,
                    projectId=projectId
                ),
                requirements=requirements,
                preferedLanguage=JobRequestUtil._get_prefered_language_by_requirements(requirements)
            )
        )
        return state
    
    @staticmethod
    def _get_prefered_language_by_requirements(requirements: str) -> str:
        if re.search(r'[가-힣]', requirements):
            return "Korean"
        return "English"

    @staticmethod
    def _make_event_storming_link(job_id: str) -> str:
        dbuid = JobRequestUtil._dbuid()
        msaez_es_url = Config.get_msaez_es_url(dbuid)
        return f"{msaez_es_url}?jobId={job_id}"

    @staticmethod
    def _dbuid() -> str:
        def s4():
            value = int((1 + secrets.SystemRandom().random()) * 0x10000)
            return format(value, 'x')[1:]
        return ''.join([s4() for _ in range(8)])
    

    @staticmethod
    def _watch_job_status(job_id: str):
        queue: asyncio.Queue = asyncio.Queue()
        db_system = DatabaseFactory.get_db_system()


        job_logs_path = Config.get_job_logs_path(job_id)
        job_is_completed_path = Config.get_job_is_completed_path(job_id)
        job_is_failed_path = Config.get_job_is_failed_path(job_id)
        watch_paths = [job_logs_path, job_is_completed_path, job_is_failed_path]
        
        main_loop = asyncio.get_running_loop()
        def on_job_logs_change(data: Optional[Dict[str, Any]]):
            try:
                asyncio.run_coroutine_threadsafe(queue.put({
                    "type": "logs",
                    "data": data
                }), main_loop)
            except Exception as e:
                SmartLogger.log("ERROR", "Queue 푸시 실패", category=CATEGORY, params={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                })
        
        def on_job_is_completed_change(data: Optional[Dict[str, Any]]):
            asyncio.run_coroutine_threadsafe(queue.put({
                "type": "is_completed",
                "data": data
            }), main_loop)
        
        def on_job_is_failed_change(data: Optional[Dict[str, Any]]):
            asyncio.run_coroutine_threadsafe(queue.put({
                "type": "is_failed",
                "data": data
            }), main_loop)

        db_system.watch_data(job_logs_path, on_job_logs_change)
        db_system.watch_data(job_is_completed_path, on_job_is_completed_change)
        db_system.watch_data(job_is_failed_path, on_job_is_failed_change)


        def clear_watch_paths():
            db_system = DatabaseFactory.get_db_system()
            for path in watch_paths:
                try:
                    db_system.unwatch_data(path)
                except Exception as e:
                    SmartLogger.log("ERROR", "Watch 해제 실패", category=CATEGORY, params={
                        "path": path,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    })

        return {
            "queue": queue,
            "watch_paths": watch_paths,
            "clear_watch_paths": clear_watch_paths,
        }