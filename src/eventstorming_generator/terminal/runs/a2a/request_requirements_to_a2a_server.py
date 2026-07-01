import httpx
from a2a.client import A2AClient, A2ACardResolver, create_text_message_object
from a2a.types import (
    Role,
    MessageSendParams,
    SendStreamingMessageRequest,
    TaskStatusUpdateEvent,
    TaskArtifactUpdateEvent,
    Task,
    Message,
)

from ....utils import LoggingUtil
from ...terminal_helper import TerminalHelper
from ..run_helper import RunHelper
from ..mocks import request_requirements_to_a2a_server_inputs

async def request_requirements_to_a2a_server(command_args):
    run_name = "request_requirements_to_a2a_server"

    BASE_URL = RunHelper.input_with_default("A2A 서버 URL", "http://localhost:5000")
    REQUIREMENTS = request_requirements_to_a2a_server_inputs.get("requirements")

    logs = []
    def print_callback(message: str = ""):
        logs.append(message)
        print(message)

    try:
        await _streaming_request(BASE_URL, REQUIREMENTS, print_callback)
    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
    
    TerminalHelper.save_dict_to_temp_file(logs, f"{run_name}_logs")

async def _streaming_request(base_url: str, requirements: str, print_callback: callable):
    """
    스트리밍 요청 테스트
    
    서버에 스트리밍 요청을 보내고, 실시간으로 응답 청크를 수신합니다.
    
    Args:
        name: 인사할 사용자 이름
    """
    async with httpx.AsyncClient(timeout=None) as httpx_client:
        try:
            # 1. AgentCard 가져오기
            card_resolver = A2ACardResolver(
                httpx_client=httpx_client,
                base_url=base_url,
            )
            agent_card = await card_resolver.get_agent_card()
            
            print_callback(f"📇 에이전트: {agent_card.name}")
            print_callback(f"📡 스트리밍 지원: {agent_card.capabilities.streaming}")
            
            if not agent_card.capabilities.streaming:
                print_callback("⚠️  이 에이전트는 스트리밍을 지원하지 않습니다!")
                return


            # 2. A2AClient 생성
            client = A2AClient(
                httpx_client=httpx_client,
                agent_card=agent_card,
            )
            
            # 3. 메시지 생성
            message = create_text_message_object(
                role=Role.user,
                content=requirements,
            )
            
            print_callback(f"\n📤 스트리밍 요청 전송 길이: {len(requirements)}")
            print_callback("-" * 40)
            

            # 4. SendStreamingMessageRequest 생성
            request = SendStreamingMessageRequest(
                id="streaming-1",
                params=MessageSendParams(message=message),
            )
            
            # 5. 스트리밍 응답 수신
            full_response = ""
            event_count = 0
            
            async for response in client.send_message_streaming(request):
                event_count += 1
                
                # response는 SendStreamingMessageResponse 타입
                # response.root가 실제 응답 객체 (SendStreamingMessageSuccessResponse)
                # response.root.result가 실제 이벤트 데이터
                actual_response = response.root if hasattr(response, 'root') else response
                result = actual_response.result if hasattr(actual_response, 'result') else None
                
                if result is None:
                    print_callback(f"⚠️  result 없음: {response}")
                    continue
                    
                if isinstance(result, TaskStatusUpdateEvent):
                    # 상태 업데이트 이벤트
                    state = result.status.state if result.status else "unknown"
                    is_final = result.final
                    print_callback(f"📊 상태: {state} (final: {is_final})")
                    
                elif isinstance(result, TaskArtifactUpdateEvent):
                    # Artifact 업데이트 이벤트 (실제 응답 데이터)
                    artifact = result.artifact
                    is_last_chunk = result.last_chunk
                    
                    if artifact and artifact.parts:
                        for part in artifact.parts:
                            # part는 Part 타입이고 part.root가 실제 TextPart
                            actual_part = part.root if hasattr(part, 'root') else part
                            if hasattr(actual_part, 'text'):
                                chunk_text = actual_part.text
                                full_response += chunk_text
                                # 실시간으로 청크 출력 (줄바꿈 없이)
                                print_callback(chunk_text)
                    
                    if is_last_chunk:
                        print_callback()  # 마지막 청크 후 줄바꿈
                        print_callback(f"✅ 마지막 청크 수신")
                        
                elif isinstance(result, Task):
                    # 최종 Task 결과 (비스트리밍 응답 또는 최종 상태)
                    print_callback(f"📋 Task ID: {result.id}")
                    print_callback(f"📊 최종 상태: {result.status.state if result.status else 'N/A'}")
                    
                elif isinstance(result, Message):
                    # 메시지 응답
                    print_callback(f"💬 메시지 수신")
                    if result.parts:
                        for part in result.parts:
                            actual_part = part.root if hasattr(part, 'root') else part
                            if hasattr(actual_part, 'text'):
                                print_callback(f"   텍스트: {actual_part.text}")
                                
                else:
                    print_callback(f"❓ 알 수 없는 응답 타입: {type(result)}")
            
            print_callback("-" * 40)
            print_callback(f"\n📈 통계:")
            print_callback(f"   총 이벤트 수: {event_count}")
            print_callback(f"   전체 응답: {full_response}")
            
        except Exception as e:
            print_callback(f"❌ 에러 발생: {str(e)}")
            import traceback
            traceback.print_exc()
            print_callback("💡 서버가 실행 중인지 확인하세요")
