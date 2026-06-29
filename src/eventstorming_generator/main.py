import asyncio
import concurrent.futures
import threading
import multiprocessing
import os
import sys
import traceback
from dotenv import load_dotenv
load_dotenv()

from eventstorming_generator.utils import LogUtil, LoggingUtil
from eventstorming_generator.utils.job_utils import JobUtil, DecentralizedJobManager, A2ASessionManager
from eventstorming_generator.models import State
from eventstorming_generator.config import Config
from eventstorming_generator.run_a2a_server import run_a2a_server
from eventstorming_generator.simple_autoscaler import start_autoscaler
from eventstorming_generator.systems.database.database_factory import DatabaseFactory

# 전역 job_manager 인스턴스 (process_job_async에서 접근하기 위함)
_current_job_manager: DecentralizedJobManager = None

# multiprocessing context: fork(기본) 은 부모의 SQLite/httpx/asyncio 핸들을 자식에 그대로
# 상속시켜 deadlock·SIGSEGV 등 silent failure 를 일으킨다 (특히 SQLiteCache).
# spawn 은 새 Python 인터프리터를 띄워 모든 자원을 자식이 자체 초기화하므로 안전하다.
_MP_CTX = multiprocessing.get_context("spawn")


def _run_graph_in_subprocess(state_dict):
    """별도 프로세스에서 graph.invoke 실행.

    spawn 모드에서는 자식이 모듈을 새로 import 하므로, 부모 상태를 그대로 잇지 않는다.
    여기서는 자식 진입과 종료/예외 모두를 stdout 에 즉시 flush 하여
    컨테이너 로그에서 자식이 실제로 어디까지 갔는지 보이도록 한다.
    """
    # 라인 버퍼링 강제 — 자식 stdout 이 block-buffer 라 어느 시점에 멈춰도 로그가 안 비치는 것 방지
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError):
        pass

    job_label = state_dict.get("inputs", {}).get("jobId", "<unknown>") if isinstance(state_dict, dict) else "<unknown>"
    print(f"[subprocess] graph 자식 프로세스 진입 (job={job_label}, pid={os.getpid()})", flush=True)

    try:
        from eventstorming_generator.models import State
        from eventstorming_generator.utils.job_utils import JobUtil
        from eventstorming_generator.graph import graph

        print(f"[subprocess] import 완료, 상태 복원 중 (job={job_label})", flush=True)
        state = State(**state_dict)
        state = JobUtil.add_element_ref_to_state(state)

        print(f"[subprocess] graph.invoke 호출 (job={job_label})", flush=True)
        graph.invoke(state, {"recursion_limit": 2147483647})
        print(f"[subprocess] graph.invoke 정상 종료 (job={job_label})", flush=True)
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        # 모든 예외(SystemExit/KeyboardInterrupt 포함)를 stderr 로 즉시 flush 하여 silent crash 방지
        sys.stderr.write(f"[subprocess][ERROR] graph 자식 프로세스 예외 (job={job_label}): {e!r}\n")
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        # 비정상 종료 코드로 부모에게 알림 (parent 의 exitcode 검사로 RuntimeError 발생 → 로그 노출)
        sys.exit(1)

async def main():
    """메인 함수 - A2A 서버 (헬스체크 포함), Job 모니터링, 자동 스케일러 동시 시작"""
    
    a2a_thread = None
    restart_count = 0
    
    while True:
        tasks = []
        job_manager = None
        
        try:
            
            # A2A 서버 시작 (헬스체크 엔드포인트 포함, 첫 실행시에만)
            if a2a_thread is None:
                a2a_thread = threading.Thread(target=run_a2a_server, daemon=True)
                a2a_thread.start()
                a2a_host = os.getenv('A2A_HOST', 'localhost')
                a2a_port = os.getenv('A2A_PORT', '5000')
                LoggingUtil.info("main", f"A2A 서버가 포트 {a2a_port}에서 시작되었습니다.")
                LoggingUtil.info("main", f"헬스체크 엔드포인트: http://{a2a_host}:{a2a_port}/ok")
                LoggingUtil.info("main", f"A2A 엔드포인트: http://{a2a_host}:{a2a_port}/")
                LoggingUtil.info("main", f"Agent Card: http://{a2a_host}:{a2a_port}/.well-known/agent.json")

            if restart_count > 0:
                LoggingUtil.info("main", f"메인 함수 재시작 중... (재시작 횟수: {restart_count})")

            pod_id = Config.get_pod_id()
            job_manager = DecentralizedJobManager(pod_id, process_job_async)
            
            # 전역 job_manager 설정
            global _current_job_manager
            _current_job_manager = job_manager
            
            if Config.is_local_run():
                tasks.append(asyncio.create_task(job_manager.start_job_monitoring()))
                LoggingUtil.info("main", "작업 모니터링이 시작되었습니다.")
            else:
                tasks.append(asyncio.create_task(start_autoscaler()))
                tasks.append(asyncio.create_task(job_manager.start_job_monitoring()))
                LoggingUtil.info("main", "자동 스케일러 및 작업 모니터링이 시작되었습니다.")
            
            
            # shutdown_event 모니터링 태스크 추가
            shutdown_monitor_task = asyncio.create_task(job_manager.shutdown_event.wait())
            tasks.append(shutdown_monitor_task)
            
            # 태스크들 중 하나라도 완료되면 종료 (shutdown_event 포함)
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # shutdown_event가 설정되었는지 확인
            if shutdown_monitor_task in done:
                LoggingUtil.info("main", "Graceful shutdown 신호 수신. 메인 루프를 종료합니다.")
                
                # A2A 세션이 활성화되어 있으면 종료될 때까지 대기
                a2a_session_manager = A2ASessionManager.instance()
                if a2a_session_manager.has_active_sessions():
                    active_count = a2a_session_manager.get_active_session_count()
                    LoggingUtil.info("main", f"활성 A2A 세션이 {active_count}개 있어 종료를 대기합니다...")
                    
                    while a2a_session_manager.has_active_sessions():
                        active_count = a2a_session_manager.get_active_session_count()
                        await asyncio.sleep(5)
                    
                    LoggingUtil.info("main", "모든 A2A 세션이 종료되었습니다.")
                
                # 나머지 실행 중인 태스크들 취소
                for task in pending:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as cleanup_error:
                            LoggingUtil.exception("main", "태스크 정리 중 예외 발생", cleanup_error)
                
                LoggingUtil.info("main", "메인 함수 정상 종료")
                break  # while 루프 종료
            
        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
            restart_count += 1
            LoggingUtil.exception("main", f"메인 함수에서 예외 발생 (재시작 횟수: {restart_count})", e)
            
            # 실행 중인 태스크들 정리
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as cleanup_error:
                        LoggingUtil.exception("main", "태스크 정리 중 예외 발생", cleanup_error)

            continue

async def process_job_async(job_id: str, complete_job_func: callable):
    """비동기 Job 처리 함수"""
    
    try:

        if not JobUtil.is_valid_job_id(job_id):
            LoggingUtil.warning("main", f"Job 처리 오류: {job_id}, 유효하지 않음")
            return
        
        # 데이터 로딩을 executor에서 실행하여 이벤트 루프 블록킹 방지
        # DatabaseFactory는 이미 파일 상단에서 import됨
        db_system = DatabaseFactory.get_db_system()
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as initial_executor:
            job_data = await loop.run_in_executor(
                initial_executor, 
                lambda: db_system.get_data(Config.get_job_path(job_id))
            )
        
        if not job_data:
            LoggingUtil.warning("main", f"Job 처리 오류: {job_id}, 데이터 없음")
            return

        state = JobUtil.get_state_from_job_data_safely(job_data) 
        if not state:
            LoggingUtil.warning("main", f"Job 처리 오류: {job_id} - State 데이터 변환 실패")
            return False

        # 이벤트 루프 양보 - 모니터링 등 다른 태스크들이 실행될 수 있도록 함
        await asyncio.sleep(0)

        # 전역 job_manager에서 취소 이벤트 가져오기
        global _current_job_manager
        cancellation_event = None
        if _current_job_manager:
            cancellation_event = _current_job_manager.get_job_cancellation_event(job_id)

        # 서브프로세스에서 그래프 실행을 비동기로 관리 (spawn 컨텍스트 사용 — 위 _MP_CTX 주석 참고)
        process = _MP_CTX.Process(target=_run_graph_in_subprocess, args=(state.model_dump(),))
        process.start()

        try:
            while process.is_alive():
                await asyncio.sleep(0.1)

                # 취소 신호 확인
                cancellation_checks = []
                current_task = asyncio.current_task()
                if current_task and current_task.cancelled():
                    cancellation_checks.append("current_task_cancelled")
                if _current_job_manager and _current_job_manager.is_job_cancelled(job_id):
                    cancellation_checks.append("job_manager_flag")
                if cancellation_event and cancellation_event.is_set():
                    cancellation_checks.append("cancellation_event")

                if cancellation_checks:
                    try:
                        process.terminate()
                    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError):
                        pass
                    raise asyncio.CancelledError()

            # 프로세스 종료 코드 확인
            exitcode = process.exitcode
            if exitcode not in (0, None):
                raise RuntimeError(f"Graph subprocess exited with code {exitcode}")
        finally:
            if process.is_alive():
                try:
                    process.terminate()
                except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError):
                    pass
            process.join(timeout=1)
            
        
    except asyncio.CancelledError:
        # 작업이 취소된 경우 (삭제 요청 등)
        
        # 취소된 경우에는 complete_job_func를 호출하지 않음 (DecentralizedJobManager에서 처리)
        return
        
    except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
        LoggingUtil.exception("main", f"Job 처리 오류: {job_id}", e)

        # state가 초기화되었는지 확인 후 오류 처리
        try:
            # job_data를 다시 로드하여 state 생성 시도
            db_system = DatabaseFactory.get_db_system()
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                job_data = await loop.run_in_executor(
                    executor, 
                    lambda: db_system.get_data(Config.get_job_path(job_id))
                )
            
            if job_data:
                state = JobUtil.get_state_from_job_data_safely(job_data)
                if state:
                    state.outputs.isFailed = True
                    LogUtil.add_exception_object_log(state, f"Job 처리 오류: {job_id}", e)
                    JobUtil.update_job_to_firebase_fire_and_forget(state)
        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as state_error:
            LoggingUtil.exception("main", f"Job 오류 처리 중 state 생성 실패: {job_id}", state_error)
        
    finally:
        # 작업 완료 후 항상 리소스 정리
        try:

            
            # cleanup_job_resources를 executor에서 비동기로 실행
            # (내부의 time.sleep이 asyncio 이벤트 루프를 블록하지 않도록)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: JobUtil.cleanup_job_resources(job_id))
            
            # 정상 완료된 경우에만 requestedJob 삭제 및 complete_job_func 호출
            # 취소된 경우에는 DecentralizedJobManager에서 처리하므로 여기서는 하지 않음
            current_task = asyncio.current_task()
            if current_task and not current_task.cancelled():
                job_request_path = Config.get_requested_job_path(job_id)
                db_system = DatabaseFactory.get_db_system()
                # Fire and forget 방식으로 삭제 (비동기 실행)
                asyncio.create_task(asyncio.to_thread(db_system.delete_data, job_request_path))
                complete_job_func()
            else:
                pass

        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as cleanup_error:
            LoggingUtil.exception("main", f"Job 정리 오류: {job_id}", cleanup_error)

if __name__ == "__main__":
    asyncio.run(main())
