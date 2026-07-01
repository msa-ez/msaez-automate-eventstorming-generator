"""
AgentExecutor 구현 모듈
이벤트 스토밍 생성 태스크를 처리합니다.
Webhook 유무에 따라 두 가지 모드로 동작합니다.

Webhook URL이 있는 경우 (Push Notification 모드):
1. 클라이언트 요청 수신 (with webhook URL)
2. 작업 큐에 추가하고 URL 반환 (working 상태)
3. 백그라운드에서 Firebase watch로 작업 완료 감지
4. 15분 주기 HealthCheck (working 상태 유지 알림)
5. 작업 완료 시 completed 알림 (webhook 통해 전송)

Webhook URL이 없는 경우 (즉시 완료 모드):
1. 클라이언트 요청 수신
2. 작업 큐에 추가하고 URL 반환
3. 즉시 completed 상태로 응답 (작업 완료 대기 없음)
"""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Optional
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.types import (
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
    TaskArtifactUpdateEvent,
    Artifact,
    TextPart,
    Message,
    Part,
    Role,
)

from ..utils.job_utils import JobRequestUtil, A2ASessionManager
from ..utils.smart_logger import SmartLogger
from ..config import Config
from ..systems.database.database_factory import DatabaseFactory


# 로깅 카테고리
CATEGORY = "agent_executor"

# 15분 (900초)
HEARTBEAT_INTERVAL_SEC = 900


class EventStormingAgentExecutor(AgentExecutor):
    """
    이벤트 스토밍 생성 에이전트 실행기
    요구사항을 받아 이벤트 스토밍 다이어그램 생성을 처리합니다.
    
    Webhook URL 유무에 따라 두 가지 모드로 동작:
    
    1. Push Notification 모드 (Webhook URL 있음):
       - 작업 요청 후 URL 반환 (working 상태)
       - Firebase watch로 작업 완료 감지 (execute 메서드 내에서 await)
       - 15분 주기 HealthCheck로 클라이언트에 working 상태 알림
       - 작업 완료 시 completed 알림 (webhook 통해 전송)
       - 주의: execute 메서드가 작업 완료까지 대기해야 EventQueue 처리가 계속됩니다.
       - 클라이언트는 blocking=False로 요청하므로 서버 응답을 기다리지 않습니다.
    
    2. 즉시 완료 모드 (Webhook URL 없음):
       - 작업 요청 후 URL 반환
       - 즉시 completed 상태로 응답 (작업 완료 대기 없음)
       - 클라이언트는 반환된 URL에서 직접 결과 확인
    """
    
    def _has_push_notification(self, context: RequestContext) -> bool:
        """
        Push Notification(Webhook) 설정이 있는지 확인합니다.
        
        Args:
            context: 요청 컨텍스트
            
        Returns:
            bool: push_notification_config.url이 설정되어 있으면 True
        """
        config = context.configuration
        if config is None:
            return False
        push_config = config.push_notification_config
        if push_config is None:
            return False
        return push_config.url is not None
    
    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        """
        Task를 처리하는 메인 메서드
        Webhook 기반 비동기 처리로 작업 요청 후 URL을 반환하고,
        백그라운드에서 작업 완료를 감지하여 클라이언트에 알림합니다.
        
        Args:
            context: 요청 컨텍스트 (사용자 메시지, task ID 등 포함)
            event_queue: 이벤트를 발행할 큐
        """
        task_id = context.task_id
        context_id = context.context_id
        start_time = datetime.now()
        
        SmartLogger.log("DEBUG", "Task 수신", category=CATEGORY, params={
            "task_id": task_id,
            "context_id": context_id,
        })
        
        # 1. 사용자 입력 가져오기 (requirements)
        requirements = context.get_user_input()
        requirements_preview = requirements[:100] + "..." if len(requirements) > 100 else requirements
        SmartLogger.log("DEBUG", "요구사항 수신", category=CATEGORY, params={
            "task_id": task_id,
            "requirements_length": len(requirements),
            "requirements_preview": requirements_preview,
        })
        
        if not requirements or not requirements.strip():
            await self._send_error(
                task_id, context_id, event_queue,
                "이벤트 스토밍 생성을 위한 요구사항이 필요합니다."
            )
            return
        
        # 2. Webhook 유무에 따라 다른 처리 방식 선택
        has_webhook = self._has_push_notification(context)
        
        SmartLogger.log("INFO", "처리 모드 결정", category=CATEGORY, params={
            "task_id": task_id,
            "has_webhook": has_webhook,
            "mode": "PUSH_NOTIFICATION" if has_webhook else "IMMEDIATE_COMPLETE",
        })
        
        if has_webhook:
            # Webhook URL이 있으면: 작업 완료까지 모니터링
            await self._execute_with_webhook(
                task_id, context_id, requirements, event_queue, start_time
            )
        else:
            # Webhook URL이 없으면: URL 반환 후 즉시 완료 처리
            await self._execute_immediate_complete(
                task_id, context_id, requirements, event_queue, start_time
            )

    async def _execute_with_webhook(
        self,
        task_id: str,
        context_id: str,
        requirements: str,
        event_queue: EventQueue,
        start_time: datetime
    ) -> None:
        """
        Webhook 기반 비동기 처리 메인 로직
        
        1. 작업 큐에 추가하고 job_id, link 획득
        2. URL 반환 메시지를 포함한 working 상태 전송
        3. 백그라운드 모니터링 태스크 시작 (Firebase watch + heartbeat)
        4. 작업 완료 시 completed 상태 전송
        """
        SmartLogger.log("INFO", "Webhook 기반 실행 시작", category=CATEGORY, params={
            "task_id": task_id,
            "requirements_length": len(requirements),
        })
        
        try:
            # 1. 작업 큐에 추가하고 job_id, link 획득
            SmartLogger.log("DEBUG", "작업 큐에 요청 제출 중", category=CATEGORY, params={
                "task_id": task_id,
            })
            
            job_id, link = JobRequestUtil.add_job_request_by_requirements(requirements)
            
            SmartLogger.log("INFO", "작업 큐 추가 성공", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "link": link,
            })
            
            # 2. 응답 메시지 생성 (URL 반환)
            response_text = (
                f"The event storming creation request has been added to the work queue. "
                f"Check your progress and results at the following link: {link}"
            )
            
            message_id = str(uuid.uuid4())
            response_message = Message(
                message_id=message_id,
                role=Role.agent,
                parts=[Part(root=TextPart(text=response_text))],
                task_id=task_id,
                context_id=context_id,
            )
            
            SmartLogger.log("DEBUG", "응답 메시지 생성 완료", category=CATEGORY, params={
                "task_id": task_id,
                "message_id": message_id,
            })
            
            # 3. WORKING 상태 + 응답 메시지 전송 (클라이언트에게 URL 반환)
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    status=TaskStatus(
                        state=TaskState.working,
                        message=response_message,
                    ),
                    final=False,  # 아직 완료 아님 - 백그라운드 모니터링 시작
                )
            )
            
            SmartLogger.log("INFO", "URL 반환 완료 (working 상태)", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "state": "working",
                "final": False,
            })
            
            # 4. 모니터링 실행 (Firebase watch + 15분 heartbeat)
            # 주의: await로 실행해야 EventQueue 처리가 계속됨
            # 클라이언트는 blocking=False로 요청하므로 서버 응답을 기다리지 않음
            SmartLogger.log("INFO", "모니터링 시작 (작업 완료까지 대기)", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "heartbeat_interval_sec": HEARTBEAT_INTERVAL_SEC,
            })
            
            await self._start_background_monitoring(
                task_id, context_id, job_id, link, event_queue, start_time
            )
            
        except Exception as e:
            SmartLogger.log("ERROR", "Webhook 실행 오류", category=CATEGORY, params={
                "task_id": task_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
            })
            await self._send_error(task_id, context_id, event_queue, str(e))

    async def _execute_immediate_complete(
        self,
        task_id: str,
        context_id: str,
        requirements: str,
        event_queue: EventQueue,
        start_time: datetime
    ) -> None:
        """
        Webhook URL이 없는 경우: 작업 큐 추가 후 URL 반환하고 즉시 완료 처리
        
        클라이언트는 반환된 URL에서 직접 작업 진행 상황과 결과를 확인합니다.
        서버는 작업 완료를 기다리지 않고 즉시 응답합니다.
        
        1. 작업 큐에 추가하고 job_id, link 획득
        2. URL 반환 메시지 생성
        3. 즉시 completed 상태 전송 (모니터링 없음)
        """
        SmartLogger.log("INFO", "즉시 완료 모드 실행 시작", category=CATEGORY, params={
            "task_id": task_id,
            "requirements_length": len(requirements),
            "mode": "IMMEDIATE_COMPLETE",
        })
        
        try:
            # 1. 작업 큐에 추가하고 job_id, link 획득
            SmartLogger.log("DEBUG", "작업 큐에 요청 제출 중", category=CATEGORY, params={
                "task_id": task_id,
            })
            
            job_id, link = JobRequestUtil.add_job_request_by_requirements(requirements)
            
            SmartLogger.log("INFO", "작업 큐 추가 성공", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "link": link,
            })
            
            # 2. 응답 메시지 생성 (URL 반환)
            response_text = (
                f"The event storming creation request has been added to the work queue. "
                f"Check your progress and results at the following link: {link}"
            )
            
            message_id = str(uuid.uuid4())
            response_message = Message(
                message_id=message_id,
                role=Role.agent,
                parts=[Part(root=TextPart(text=response_text))],
                task_id=task_id,
                context_id=context_id,
            )
            
            SmartLogger.log("DEBUG", "응답 메시지 생성 완료", category=CATEGORY, params={
                "task_id": task_id,
                "message_id": message_id,
            })
            
            # 3. WORKING 상태 + 응답 메시지 전송 (history에 메시지 추가)
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    status=TaskStatus(
                        state=TaskState.working,
                        message=response_message,
                    ),
                    final=False,
                )
            )
            
            # 4. 즉시 COMPLETED 상태로 변경 (모니터링 없음)
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    status=TaskStatus(state=TaskState.completed),
                    final=True,
                )
            )
            
            total_duration = (datetime.now() - start_time).total_seconds()
            SmartLogger.log("INFO", "즉시 완료 처리 완료", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "link": link,
                "total_duration_sec": round(total_duration, 3),
                "mode": "IMMEDIATE_COMPLETE",
            })
            
        except Exception as e:
            SmartLogger.log("ERROR", "즉시 완료 처리 오류", category=CATEGORY, params={
                "task_id": task_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
            })
            await self._send_error(task_id, context_id, event_queue, str(e))

    async def _start_background_monitoring(
        self,
        task_id: str,
        context_id: str,
        job_id: str,
        link: str,
        event_queue: EventQueue,
        start_time: datetime
    ) -> None:
        """
        작업 완료를 모니터링하고 webhook 알림을 전송합니다.
        
        - Firebase watch로 작업 완료/실패 감지
        - 15분 주기 heartbeat로 클라이언트에 working 상태 알림
        - 작업 완료 시 completed/failed 상태 전송
        
        주의: 이 메서드는 작업 완료까지 await 되어야 합니다.
        """
        session_id = str(uuid.uuid4())
        session_manager = A2ASessionManager.instance()
        
        # asyncio.Queue를 사용하여 Firebase 콜백과 통신
        status_queue: asyncio.Queue = asyncio.Queue()
        heartbeat_count = 0
        
        try:
            # 세션 등록
            session_manager.register_session(session_id)
            SmartLogger.log("DEBUG", "세션 등록 완료", category=CATEGORY, params={
                "task_id": task_id,
                "session_id": session_id,
            })
            
            # DB watch 설정
            db_system = DatabaseFactory.get_db_system()
            job_is_completed_path = Config.get_job_is_completed_path(job_id)
            job_is_failed_path = Config.get_job_is_failed_path(job_id)
            watch_paths = [job_is_completed_path, job_is_failed_path]
            
            # 현재 이벤트 루프 참조
            main_loop = asyncio.get_running_loop()
            
            def on_completed_change(data):
                if data:
                    asyncio.run_coroutine_threadsafe(
                        status_queue.put({"type": "completed", "data": data}),
                        main_loop
                    )
            
            def on_failed_change(data):
                if data:
                    asyncio.run_coroutine_threadsafe(
                        status_queue.put({"type": "failed", "data": data}),
                        main_loop
                    )
            
            # DB watch 시작
            db_system.watch_data(job_is_completed_path, on_completed_change)
            db_system.watch_data(job_is_failed_path, on_failed_change)
            
            SmartLogger.log("DEBUG", "DB watch 시작", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "watch_paths": watch_paths,
            })
            
            # 모니터링 루프 (heartbeat + 상태 감지)
            while True:
                try:
                    # 15분 타임아웃으로 상태 변화 대기
                    event = await asyncio.wait_for(
                        status_queue.get(),
                        timeout=HEARTBEAT_INTERVAL_SEC
                    )
                    
                    event_type = event.get("type")
                    
                    if event_type == "completed":
                        # 작업 완료
                        await self._send_completion(
                            task_id, context_id, job_id, link, event_queue, start_time
                        )
                        break
                        
                    elif event_type == "failed":
                        # 작업 실패
                        await self._send_failure(
                            task_id, context_id, job_id, link, event_queue, start_time
                        )
                        break
                        
                except asyncio.TimeoutError:
                    # 15분 경과 - Heartbeat 전송
                    heartbeat_count += 1
                    await self._send_heartbeat(
                        task_id, context_id, job_id, link, event_queue, heartbeat_count
                    )
                    
                    # 직접 상태 확인 (watch 콜백이 누락된 경우 대비)
                    current_completed = db_system.get_data(job_is_completed_path)
                    current_failed = db_system.get_data(job_is_failed_path)
                    
                    if current_completed:
                        await self._send_completion(
                            task_id, context_id, job_id, link, event_queue, start_time
                        )
                        break
                    elif current_failed:
                        await self._send_failure(
                            task_id, context_id, job_id, link, event_queue, start_time
                        )
                        break
                        
        except asyncio.CancelledError:
            SmartLogger.log("INFO", "모니터링 취소됨", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
            })
            raise
            
        except Exception as e:
            SmartLogger.log("ERROR", "모니터링 오류", category=CATEGORY, params={
                "task_id": task_id,
                "job_id": job_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
            })
            await self._send_error(task_id, context_id, event_queue, f"모니터링 오류: {str(e)}")
            
        finally:
            # Watch 해제
            try:
                db_system = DatabaseFactory.get_db_system()
                for path in watch_paths:
                    try:
                        db_system.unwatch_data(path)
                    except Exception:
                        pass
            except Exception:
                pass
            
            # 세션 해제
            session_manager.unregister_session(session_id)
            
            SmartLogger.log("DEBUG", "세션 해제 완료", category=CATEGORY, params={
                "task_id": task_id,
                "session_id": session_id,
            })

    async def _send_heartbeat(
        self,
        task_id: str,
        context_id: str,
        job_id: str,
        link: str,
        event_queue: EventQueue,
        heartbeat_count: int
    ) -> None:
        """
        15분 주기 HealthCheck - working 상태 유지 알림
        클라이언트에게 작업이 진행 중임을 알립니다.
        """
        elapsed_minutes = heartbeat_count * (HEARTBEAT_INTERVAL_SEC // 60)
        
        heartbeat_text = (
            f"[HealthCheck] 이벤트 스토밍 생성 작업이 진행 중입니다. "
            f"(경과 시간: 약 {elapsed_minutes}분) "
            f"진행 상황: {link}"
        )
        
        message_id = str(uuid.uuid4())
        heartbeat_message = Message(
            message_id=message_id,
            role=Role.agent,
            parts=[Part(root=TextPart(text=heartbeat_text))],
            task_id=task_id,
            context_id=context_id,
        )
        
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(
                    state=TaskState.working,
                    message=heartbeat_message,
                ),
                final=False,
            )
        )
        
        SmartLogger.log("INFO", "Heartbeat 전송", category=CATEGORY, params={
            "task_id": task_id,
            "job_id": job_id,
            "heartbeat_count": heartbeat_count,
            "elapsed_minutes": elapsed_minutes,
        })

    async def _send_completion(
        self,
        task_id: str,
        context_id: str,
        job_id: str,
        link: str,
        event_queue: EventQueue,
        start_time: datetime
    ) -> None:
        """
        작업 완료 알림
        """
        total_duration = (datetime.now() - start_time).total_seconds()
        
        completion_text = (
            f"Event Storming generation complete! "
            f"You can check the results at the following link: {link}"
        )
        
        message_id = str(uuid.uuid4())
        completion_message = Message(
            message_id=message_id,
            role=Role.agent,
            parts=[Part(root=TextPart(text=completion_text))],
            task_id=task_id,
            context_id=context_id,
        )
        
        # WORKING 상태로 메시지 전송 (history에 추가)
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(
                    state=TaskState.working,
                    message=completion_message,
                ),
                final=False,
            )
        )
        
        # COMPLETED 상태로 변경
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.completed),
                final=True,
            )
        )
        
        SmartLogger.log("INFO", "작업 완료", category=CATEGORY, params={
            "task_id": task_id,
            "job_id": job_id,
            "total_duration_sec": round(total_duration, 2),
            "link": link,
        })

    async def _send_failure(
        self,
        task_id: str,
        context_id: str,
        job_id: str,
        link: str,
        event_queue: EventQueue,
        start_time: datetime
    ) -> None:
        """
        작업 실패 알림
        """
        total_duration = (datetime.now() - start_time).total_seconds()
        
        failure_text = (
            f"이벤트 스토밍 생성 작업이 실패했습니다. "
            f"상세 정보는 다음 링크에서 확인하세요: {link}"
        )
        
        message_id = str(uuid.uuid4())
        failure_message = Message(
            message_id=message_id,
            role=Role.agent,
            parts=[Part(root=TextPart(text=failure_text))],
            task_id=task_id,
            context_id=context_id,
        )
        
        # WORKING 상태로 메시지 전송 (history에 추가)
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(
                    state=TaskState.working,
                    message=failure_message,
                ),
                final=False,
            )
        )
        
        # FAILED 상태로 변경
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.failed),
                final=True,
            )
        )
        
        SmartLogger.log("WARNING", "작업 실패", category=CATEGORY, params={
            "task_id": task_id,
            "job_id": job_id,
            "total_duration_sec": round(total_duration, 2),
            "link": link,
        })

    async def _send_error(
        self,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        error_message: str
    ) -> None:
        """
        에러 메시지를 발행하고 Task를 실패 상태로 변경
        """
        error_artifact = Artifact(
            artifact_id=str(uuid.uuid4()),
            parts=[TextPart(text=json.dumps({
                "type": "error",
                "state": "failed",
                "message": error_message
            }, ensure_ascii=False))],
            name="error_response",
        )
        
        await event_queue.enqueue_event(
            TaskArtifactUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                artifact=error_artifact,
                last_chunk=True,
            )
        )
        
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.failed),
                final=True,
            )
        )
        
        SmartLogger.log("ERROR", "에러 전송", category=CATEGORY, params={
            "task_id": task_id,
            "error_message": error_message,
        })

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        """
        Task 취소 처리
        
        Args:
            context: 요청 컨텍스트
            event_queue: 이벤트를 발행할 큐
        """
        task_id = context.task_id
        context_id = context.context_id
        
        SmartLogger.log("INFO", "작업 취소 요청", category=CATEGORY, params={
            "task_id": task_id,
        })
        
        # 취소 상태로 변경
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.canceled),
                final=True,
            )
        )
        
        SmartLogger.log("INFO", "작업 취소 완료", category=CATEGORY, params={
            "task_id": task_id,
        })


if __name__ == "__main__":
    # 테스트: AgentExecutor 생성
    executor = EventStormingAgentExecutor()
    print("EventStormingAgentExecutor 생성 성공!")
    print(f"Executor 클래스: {executor.__class__.__name__}")
    print(f"Heartbeat 주기: {HEARTBEAT_INTERVAL_SEC}초 ({HEARTBEAT_INTERVAL_SEC // 60}분)")
