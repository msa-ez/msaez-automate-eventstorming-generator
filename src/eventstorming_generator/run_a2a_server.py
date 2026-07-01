"""
A2A 서버 실행 모듈
FastAPI 기반 A2A 서버를 시작합니다.
헬스체크 엔드포인트 및 Push Notification(Webhook) 기능을 포함합니다.
"""

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio

from a2a.server.apps.jsonrpc.fastapi_app import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, InMemoryPushNotificationConfigStore, BasePushNotificationSender
from a2a.server.events import InMemoryQueueManager, EventConsumer
from a2a.types import Task, TaskState, Message, MessageSendParams
from a2a.utils.errors import ServerError

from eventstorming_generator.a2a_modules import create_agent_card, EventStormingAgentExecutor
from eventstorming_generator.utils.smart_logger import SmartLogger
from eventstorming_generator.config import Config

# 로깅 카테고리
CATEGORY = "a2a_server"


TERMINAL_STATES = {
    TaskState.completed,
    TaskState.canceled,
    TaskState.failed,
    TaskState.rejected,
}


class WebhookFriendlyRequestHandler(DefaultRequestHandler):
    """
    DefaultRequestHandler 개선 버전:
    - blocking=False(non-blocking) 요청은 즉시 응답을 반환
    - 이후 task가 terminal 상태에 도달하면 push notification(webhook)을 한 번 더 전송

    왜 필요한가?
    - a2a-sdk의 기본 DefaultRequestHandler는 non-blocking에서 첫 이벤트(대개 working)까지만 push를 트리거하고
      나머지 이벤트(terminal 포함)는 백그라운드에서 consume만 하며 push를 다시 보내지 않습니다.
    """

    async def _send_terminal_push_when_ready(self, task_id: str) -> None:
        # terminal 상태가 될 때까지 task_store에서 폴링 후 push 전송
        interval = 0.5
        while True:
            try:
                task = await self.task_store.get(task_id)
            except Exception as e:
                SmartLogger.log(
                    "WARNING",
                    "Failed to fetch task while waiting terminal state",
                    category=CATEGORY,
                    params={"task_id": task_id, "error": str(e)},
                )
                task = None

            state = getattr(getattr(task, "status", None), "state", None)
            if task and state in TERMINAL_STATES:
                try:
                    if self._push_sender:
                        await self._push_sender.send_notification(task)
                        SmartLogger.log(
                            "INFO",
                            "Terminal push notification sent",
                            category=CATEGORY,
                            params={"task_id": task_id, "terminal_state": str(state)},
                        )
                    # in-memory store cleanup (best-effort)
                    if self._push_config_store:
                        await self._push_config_store.delete_info(task_id)
                except Exception as e:
                    SmartLogger.log(
                        "ERROR",
                        "Terminal push notification failed",
                        category=CATEGORY,
                        params={"task_id": task_id, "error": str(e)},
                    )
                return

            await asyncio.sleep(interval)

    async def on_message_send(
        self,
        params: MessageSendParams,
        context=None,
    ) -> Message | Task:
        # DefaultRequestHandler.on_message_send를 기반으로,
        # non-blocking일 때 terminal push를 백그라운드에서 추가로 전송한다.
        (
            task_manager,
            task_id,
            queue,
            result_aggregator,
            producer_task,
        ) = await self._setup_message_execution(params, context)

        consumer = EventConsumer(queue)
        producer_task.add_done_callback(consumer.agent_task_callback)

        blocking = True
        if params.configuration and params.configuration.blocking is False:
            blocking = False

        interrupted_or_non_blocking = False
        try:
            (
                result,
                interrupted_or_non_blocking,
            ) = await result_aggregator.consume_and_break_on_interrupt(
                consumer, blocking=blocking
            )
            if not result:
                raise ServerError()

            if isinstance(result, Task):
                self._validate_task_id_match(task_id, result.id)

            # 1) 기존 동작: 첫 이벤트 시점의 push (보통 working)
            await self._send_push_notification_if_needed(task_id, result_aggregator)

            # 2) 추가 동작: non-blocking이면 terminal 도달 후 push를 한 번 더 전송
            if interrupted_or_non_blocking and not blocking:
                latest = await result_aggregator.current_result
                latest_state = (
                    latest.status.state if isinstance(latest, Task) and latest.status else None
                )
                if latest_state not in TERMINAL_STATES:
                    asyncio.create_task(self._send_terminal_push_when_ready(task_id))
                else:
                    # 이미 terminal이면(매우 짧은 작업) 추가 전송 필요 없음
                    SmartLogger.log(
                        "DEBUG",
                        "Non-blocking: already terminal at first response, skip terminal push scheduling",
                        category=CATEGORY,
                        params={"task_id": task_id, "terminal_state": str(latest_state)},
                    )

        except Exception as e:
            SmartLogger.log(
                "ERROR",
                "Agent execution failed in WebhookFriendlyRequestHandler",
                category=CATEGORY,
                params={"error": str(e)},
            )
            raise
        finally:
            if interrupted_or_non_blocking:
                asyncio.create_task(self._cleanup_producer(producer_task, task_id))
            else:
                await self._cleanup_producer(producer_task, task_id)

        return result


def create_app(a2a_external_url: str) -> FastAPI:
    """
    FastAPI 애플리케이션을 생성합니다.
    
    Args:
        host: 서버 호스트
        port: 서버 포트
    
    Returns:
        FastAPI: FastAPI 앱 인스턴스
    """
    
    # 1. AgentCard 생성
    agent_card = create_agent_card(url=a2a_external_url)
    SmartLogger.log("INFO", "AgentCard 생성 완료", category=CATEGORY, params={
        "name": agent_card.name,
        "url": a2a_external_url,
        "push_notifications": agent_card.capabilities.push_notifications,
    })
    
    # 2. AgentExecutor 생성
    agent_executor = EventStormingAgentExecutor()
    SmartLogger.log("INFO", "AgentExecutor 생성 완료", category=CATEGORY, params={
        "class": agent_executor.__class__.__name__,
    })
    
    # 3. TaskStore, QueueManager 생성 (메모리 기반)
    task_store = InMemoryTaskStore()
    queue_manager = InMemoryQueueManager()
    
    # 4. Push Notification 컴포넌트 생성 (Webhook 기반 알림)
    push_config_store = InMemoryPushNotificationConfigStore()
    httpx_client = httpx.AsyncClient(timeout=30.0)
    push_sender = BasePushNotificationSender(
        httpx_client=httpx_client,
        config_store=push_config_store,
    )
    SmartLogger.log("INFO", "Push Notification 컴포넌트 생성 완료", category=CATEGORY, params={
        "feature": "webhook",
        "timeout_sec": 30.0,
    })
    
    # 5. RequestHandler 생성 (Push Notification 포함)
    request_handler = WebhookFriendlyRequestHandler(
        agent_executor=agent_executor,
        task_store=task_store,
        queue_manager=queue_manager,
        push_config_store=push_config_store,
        push_sender=push_sender,
    )
    
    # 6. A2A FastAPI 애플리케이션 생성
    a2a_app = A2AFastAPIApplication(
        agent_card=agent_card,
        http_handler=request_handler,
    )
    
    # 7. FastAPI 앱 가져오기
    app = a2a_app.build()
    
    # 8. CORS 설정
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    SmartLogger.log("INFO", "A2A 서버 설정 완료", category=CATEGORY)
    
    # 9. 헬스체크 엔드포인트 (/ok)
    @app.get("/ok")
    async def health_check():
        """헬스체크 엔드포인트"""
        return {
            "status": "ok",
            "message": "EventStorming Generator 서버가 정상 작동 중입니다."
        }
    
    # 10. 루트 엔드포인트 (서버 상태 확인용)
    @app.get("/")
    async def root():
        return {
            "message": "EventStorming Generator A2A Server is running!",
            "agent": agent_card.name,
            "version": agent_card.version,
            "capabilities": {
                "streaming": agent_card.capabilities.streaming,
                "push_notifications": agent_card.capabilities.push_notifications,
            },
            "endpoints": {
                "agent_card": "/.well-known/agent.json",
                "health_check": "/ok",
                "rpc": "/",
            }
        }
    
    return app


def run_a2a_server(host: str = "0.0.0.0", port: int = 5000):
    """
    A2A 서버를 실행합니다.
    별도 스레드에서 호출되어 동기적으로 실행됩니다.
    
    Args:
        host: 서버 호스트 (기본: 환경변수 A2A_HOST 또는 localhost)
        port: 서버 포트 (기본: 5000)
    """
    # 환경변수에서 호스트 가져오기 (없으면 기본값 사용)
    import os
    a2a_host_env = os.getenv('A2A_HOST')
    if a2a_host_env:
        host = a2a_host_env
    elif Config.is_local_run():
        host = "localhost"

    a2a_external_url = Config.a2a_external_url()
    SmartLogger.log("INFO", "A2A External URL 설정", category=CATEGORY, params={
        "external_url": a2a_external_url,
    })
    app = create_app(a2a_external_url)
    
    SmartLogger.log("INFO", "A2A 서버 시작", category=CATEGORY, params={
        "host": host,
        "port": port,
        "server_url": f"http://{host}:{port}",
        "agent_card_url": f"http://{host}:{port}/.well-known/agent.json",
        "health_check_url": f"http://{host}:{port}/ok",
    })
    
    # uvicorn 로그 레벨 설정 (헬스체크 로그 최소화)
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="warning",
    )


if __name__ == "__main__":
    run_a2a_server()
