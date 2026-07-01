import asyncio
import time
import os
import concurrent.futures
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

from .systems.database.database_factory import DatabaseFactory
from .config import Config
from .utils import LoggingUtil
from .utils.job_utils import A2ASessionManager
from eventstorming_generator.utils.catchable_exceptions import CATCHABLE_EXCEPTIONS

class SimpleAutoScaler:
    """
    Kubernetes 자동 스케일러
    
    Note: k8s 클라이언트 초기화가 GIL을 점유할 수 있으므로,
    모듈 레벨이 아닌 start_autoscaler() 호출 시 인스턴스가 생성됩니다.
    """
    
    _instance = None
    _initialized = False
    
    def __init__(self):
        # 이미 초기화된 경우 중복 초기화 방지
        if SimpleAutoScaler._initialized:
            return
            
        self.namespace = Config.autoscaler_namespace()
        self.deployment_name = Config.autoscaler_deployment_name()
        self.service_name = Config.autoscaler_service_name()
        self.min_replicas = Config.autoscaler_min_replicas()
        self.max_replicas = Config.autoscaler_max_replicas()
        self.target_jobs_per_pod = Config.autoscaler_target_jobs_per_pod()  # 대기 작업 1개당 Pod 1개
        self.scale_check_interval = 60  # 60초마다 확인
        self.scale_up_cooldown = 120   # 스케일 업 후 2분 대기
        self.scale_down_cooldown = 1800  # 스케일 다운 후 30분 대기 (작업이 길어질 수 있으므로)
        self.scale_down_grace_period = 3600  # 스케일 다운 시 1시간 추가 유예 시간
        self.last_scale_time = 0
        self.last_scale_action = None  # 'up' or 'down'
        
        # 보수적 스케일 다운을 위한 추가 변수들
        self.scale_down_observation_count = 0  # 연속으로 스케일 다운 조건을 만족한 횟수
        self.required_scale_down_observations = 5  # 스케일 다운 실행 전 필요한 관찰 횟수
        self.last_processing_jobs_count = 0  # 이전 처리 중인 작업 수
        
        # k8s API 호출을 위한 executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        
        # Kubernetes 클라이언트 초기화
        try:
            # Pod 내부에서 실행되는 경우
            config.load_incluster_config()
        except ConfigException as e:
            # 로컬에서 테스트하는 경우
            try:
                config.load_kube_config()
            except ConfigException as kube_error:
                # Kubernetes 설정이 없는 경우 (Docker Compose 환경 등)
                LoggingUtil.warning("simple_autoscaler", f"Kubernetes config를 찾을 수 없습니다. Autoscaler가 비활성화됩니다: {kube_error}")
                raise kube_error
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", "Kubernetes config 로드 중 예상치 못한 오류", e)
            raise
        
        self.apps_v1 = client.AppsV1Api()
        self.core_v1 = client.CoreV1Api()
        
        SimpleAutoScaler._initialized = True
    
    @classmethod
    def instance(cls) -> 'SimpleAutoScaler':
        """싱글톤 인스턴스 반환 (lazy initialization)"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    async def get_current_replicas_async(self) -> int:
        """현재 Deployment의 replicas 수 조회 (비동기)"""
        try:
            loop = asyncio.get_event_loop()
            deployment = await loop.run_in_executor(
                self._executor,
                lambda: self.apps_v1.read_namespaced_deployment(
                    name=self.deployment_name,
                    namespace=self.namespace
                )
            )
            return deployment.spec.replicas
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", f"현재 replicas 조회 실패: {e}", e)
            return 1
    
    async def get_active_pods_count_async(self) -> int:
        """현재 실행 중인 Pod 수 조회 (비동기, Running 상태만)"""
        try:
            loop = asyncio.get_event_loop()
            pods = await loop.run_in_executor(
                self._executor,
                lambda: self.core_v1.list_namespaced_pod(
                    namespace=self.namespace,
                    label_selector=f"app={self.deployment_name}"
                )
            )
            
            active_count = 0
            for pod in pods.items:
                if pod.status.phase == 'Running':
                    active_count += 1
            
            return active_count
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", f"활성 Pod 수 조회 실패: {e}", e)
            return 1
    
    async def set_replicas_async(self, target_replicas: int) -> bool:
        """Deployment의 replicas 수 변경 (비동기)"""
        try:
            loop = asyncio.get_event_loop()
            
            # Deployment 조회
            deployment = await loop.run_in_executor(
                self._executor,
                lambda: self.apps_v1.read_namespaced_deployment(
                    name=self.deployment_name,
                    namespace=self.namespace
                )
            )
            
            # replicas 수 변경
            deployment.spec.replicas = target_replicas
            
            # 업데이트 적용
            await loop.run_in_executor(
                self._executor,
                lambda: self.apps_v1.patch_namespaced_deployment(
                    name=self.deployment_name,
                    namespace=self.namespace,
                    body=deployment
                )
            )
            
            return True
            
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", f"replicas 변경 실패: {e}", e)
            return False
    
    def calculate_desired_replicas(self, waiting_jobs: int, processing_jobs: int) -> int:
        """대기 및 처리 중인 작업 수에 따른 목표 replicas 계산"""
        total_jobs = waiting_jobs + processing_jobs
        
        # 처리 중인 작업이 있으면 최소한 해당 수만큼은 유지
        min_required_for_processing = processing_jobs
        
        if total_jobs == 0:
            return max(self.min_replicas, min_required_for_processing)
        
        # 전체 작업 수 / 작업당 Pod 수로 계산
        desired = max(1, (total_jobs + self.target_jobs_per_pod - 1) // self.target_jobs_per_pod)
        
        # 처리 중인 작업을 위한 최소 replicas 보장
        desired = max(desired, min_required_for_processing)
        
        # min/max 범위 내로 제한
        return max(self.min_replicas, min(self.max_replicas, desired))
    
    def should_scale_up(self, current_replicas: int, desired_replicas: int) -> bool:
        """스케일 업이 필요한지 확인"""
        if desired_replicas <= current_replicas:
            return False
        
        current_time = time.time()
        time_since_last_scale = current_time - self.last_scale_time
        
        # 스케일 업 쿨다운 확인
        if self.last_scale_action == 'up' and time_since_last_scale < self.scale_up_cooldown:
            return False
        
        return True
    
    def should_scale_down(self, current_replicas: int, desired_replicas: int, processing_jobs: int) -> bool:
        """스케일 다운이 필요한지 확인 (매우 보수적 접근)"""
        if desired_replicas >= current_replicas:
            # 스케일 다운이 필요하지 않으면 관찰 카운터 리셋
            self.scale_down_observation_count = 0
            return False
        
        current_time = time.time()
        time_since_last_scale = current_time - self.last_scale_time
        
        # 기본 쿨다운 시간 확인
        if self.last_scale_action == 'down' and time_since_last_scale < self.scale_down_cooldown:
            return False
        
        # 처리 중인 작업이 있으면 스케일 다운 금지
        if processing_jobs > 0:
            self.scale_down_observation_count = 0
            return False
        
        # A2A 세션이 활성화되어 있으면 스케일 다운 금지
        try:
            a2a_session_manager = A2ASessionManager.instance()
            if a2a_session_manager.has_active_sessions():
                active_sessions = a2a_session_manager.get_active_session_count()
                self.scale_down_observation_count = 0
                return False
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", "A2A 세션 확인 중 오류", e)
        
        # 연속 관찰 카운터 증가
        self.scale_down_observation_count += 1
        
        # 충분한 관찰 횟수를 만족했는지 확인
        if self.scale_down_observation_count < self.required_scale_down_observations:
            return False
        
        # 추가 유예 시간 확인 (마지막 스케일 작업 이후)
        if time_since_last_scale < self.scale_down_grace_period:
            remaining_grace = self.scale_down_grace_period - time_since_last_scale
            return False
        
        return True
    
    async def is_leader_pod_async(self) -> bool:
        """현재 Pod가 리더인지 확인 (비동기, 가장 먼저 생성된 Pod가 리더)"""
        try:
            pod_name = os.getenv('POD_ID') or os.getenv('HOSTNAME', 'unknown')
            
            # 같은 라벨을 가진 모든 Pod 조회 (비동기)
            loop = asyncio.get_event_loop()
            pods = await loop.run_in_executor(
                self._executor,
                lambda: self.core_v1.list_namespaced_pod(
                    namespace=self.namespace,
                    label_selector=f"app={self.deployment_name}"
                )
            )
            
            if not pods.items:
                return False
            
            # 생성 시간순으로 정렬하여 가장 오래된 Pod가 리더
            sorted_pods = sorted(pods.items, key=lambda p: p.metadata.creation_timestamp)
            leader_pod_name = sorted_pods[0].metadata.name
            
            is_leader = pod_name == leader_pod_name
            if is_leader:
                pass
            
            return is_leader
            
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", f"리더 확인 실패: {e}", e)
            return False
    
    async def run_autoscaling_loop(self):
        """자동 스케일링 메인 루프 (모든 k8s API 호출이 비동기로 실행됨)"""
        LoggingUtil.info("simple_autoscaler", "고급 자동 스케일링 시작 (처리 중인 작업 보호 기능 포함)")
        
        while True:
            try:
                # 리더 Pod만 스케일링 담당 (비동기)
                if not await self.is_leader_pod_async():
                    await asyncio.sleep(self.scale_check_interval)
                    continue
                
                # 대기 및 처리 중인 작업 수 조회
                waiting_jobs = await self.get_waiting_jobs_count_async()
                processing_jobs = await self.get_processing_jobs_count_async()
                
                # 현재 replicas 및 활성 Pod 수 조회 (비동기)
                current_replicas = await self.get_current_replicas_async()
                active_pods = await self.get_active_pods_count_async()
                
                # 목표 replicas 계산
                desired_replicas = self.calculate_desired_replicas(waiting_jobs, processing_jobs)
                
                
                # 스케일링 결정
                if desired_replicas > current_replicas:
                    # 스케일 업 확인
                    if self.should_scale_up(current_replicas, desired_replicas):
                        success = await self.set_replicas_async(desired_replicas)
                        if success:
                            self.last_scale_time = time.time()
                            self.last_scale_action = 'up'
                            self.scale_down_observation_count = 0  # 스케일 업 시 관찰 카운터 리셋
                        else:
                            LoggingUtil.warning("simple_autoscaler", f"스케일 업 실패")
                    
                elif desired_replicas < current_replicas:
                    # 스케일 다운 확인 (매우 보수적)
                    if self.should_scale_down(current_replicas, desired_replicas, processing_jobs):
                        success = await self.set_replicas_async(desired_replicas)
                        if success:
                            self.last_scale_time = time.time()
                            self.last_scale_action = 'down'
                            self.scale_down_observation_count = 0  # 스케일 다운 후 카운터 리셋
                        else:
                            LoggingUtil.warning("simple_autoscaler", f"스케일 다운 실패")
                else:
                    # 스케일링이 필요하지 않은 경우
                    self.scale_down_observation_count = 0
                
                self.last_processing_jobs_count = processing_jobs
                await asyncio.sleep(self.scale_check_interval)
                
            except CATCHABLE_EXCEPTIONS as e:
                LoggingUtil.exception("simple_autoscaler", f"자동 스케일링 오류: {e}", e)
                await asyncio.sleep(self.scale_check_interval)

    async def get_waiting_jobs_count_async(self):
        """대기 중인 작업 수를 비동기로 계산"""
        try:
            # DB에서 현재 작업 데이터 조회
            db_system = DatabaseFactory.get_db_system()
            requested_jobs = await db_system.get_children_data_async(
                Config.get_requested_job_root_path()
            )
            
            if not requested_jobs:
                return 0
            
            # DecentralizedJobManager와 동일한 로직으로 대기 작업 계산
            waiting_count = 0
            for job_id, job_data in requested_jobs.items():
                # assignedPodId가 없는 작업들이 대기 중인 작업
                if job_data.get('assignedPodId') is None and job_data.get('status') != 'failed':
                    waiting_count += 1
            
            return waiting_count
            
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", "대기 작업 수 계산 오류", e)
            return 0

    async def get_processing_jobs_count_async(self):
        """처리 중인 작업 수를 비동기로 계산"""
        try:
            # DB에서 현재 작업 데이터 조회
            db_system = DatabaseFactory.get_db_system()
            requested_jobs = await db_system.get_children_data_async(
                Config.get_requested_job_root_path()
            )
            
            if not requested_jobs:
                return 0
            
            processing_count = 0
            current_time = time.time()
            
            for job_id, job_data in requested_jobs.items():
                assigned_pod = job_data.get('assignedPodId')
                last_heartbeat = job_data.get('lastHeartbeat', 0)
                status = job_data.get('status')
                
                # assignedPodId가 있고, 최근에 heartbeat가 있으며, processing 상태인 작업들
                if (assigned_pod and 
                    status == 'processing' and
                    current_time - last_heartbeat < 300):  # 5분 이내 heartbeat
                    processing_count += 1
            
            return processing_count
            
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.exception("simple_autoscaler", "처리 중인 작업 수 계산 오류", e)
            return 0

async def start_autoscaler():
    """
    자동 스케일러 시작 (lazy initialization)
    
    Note: k8s 클라이언트 초기화가 GIL을 점유할 수 있으므로,
    import 시점이 아닌 실제 사용 시점에 인스턴스를 생성합니다.
    """
    try:
        autoscaler = SimpleAutoScaler.instance()
        await autoscaler.run_autoscaling_loop()
    except ConfigException as e:
        LoggingUtil.warning("simple_autoscaler", f"Kubernetes config가 없어 autoscaler를 시작할 수 없습니다. Docker Compose 환경에서는 IS_LOCAL_RUN=true로 설정하세요: {e}")
        # Kubernetes가 없는 환경에서는 autoscaler를 시작하지 않고 무한 대기
        # 이렇게 하면 태스크가 완료되지 않아 메인 루프가 계속 실행됨
        while True:
            await asyncio.sleep(60)  # 1분마다 체크
    except CATCHABLE_EXCEPTIONS as e:
        LoggingUtil.exception("simple_autoscaler", "Autoscaler 시작 중 예상치 못한 오류", e)
        # 예상치 못한 오류도 무한 대기로 처리
        while True:
            await asyncio.sleep(60)