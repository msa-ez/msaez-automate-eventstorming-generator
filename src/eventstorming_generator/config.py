import os

class Config:
    @staticmethod
    def _split_model_string(model: str, *, default_vendor: str = "openai") -> tuple[str, str]:
        """
        모델 문자열을 vendor/name으로 분해합니다.

        지원 포맷:
        - vendor:name[:suffix...]  (예: google_genai:gemini-flash-latest:thinking)
        - name                    (예: gpt-4.1-2025-04-14)  -> (default_vendor, name)

        Note:
        - suffix(:thinking 등)는 name에서 제외하고 vendor/name만 반환합니다.
        - model이 비어있으면 ValueError를 발생시킵니다.
        """
        if not model:
            raise ValueError("AI model env var is not set (empty/None)")

        if ":" not in model:
            return default_vendor, model

        parts = model.split(":")
        vendor = parts[0] or default_vendor
        name = parts[1] if len(parts) > 1 and parts[1] else model
        return vendor, name
    @staticmethod
    def get_requested_job_root_path() -> str:
        return f"requestedJobs/{Config.get_namespace()}"
            
    @staticmethod
    def get_requested_job_path(job_id: str) -> str:
        return f"{Config.get_requested_job_root_path()}/{job_id}"


    @staticmethod
    def get_job_root_path() -> str:
        return f"jobs/{Config.get_namespace()}"

    @staticmethod
    def get_job_path(job_id: str) -> str:
        return f"{Config.get_job_root_path()}/{job_id}"


    @staticmethod
    def get_job_state_root_path() -> str:
        return f"jobStates/{Config.get_namespace()}"
    
    @staticmethod
    def get_job_state_path(job_id: str) -> str:
        return f"{Config.get_job_state_root_path()}/{job_id}"

    @staticmethod
    def get_job_logs_path(job_id: str) -> str:
        return f"{Config.get_job_path(job_id)}/state/outputs/logs"
    
    @staticmethod
    def get_job_is_completed_path(job_id: str) -> str:
        return f"{Config.get_job_path(job_id)}/state/outputs/isCompleted"
    
    @staticmethod
    def get_job_is_failed_path(job_id: str) -> str:
        return f"{Config.get_job_path(job_id)}/state/outputs/isFailed"


    @staticmethod
    def get_namespace() -> str:
        # NAMESPACE 환경 변수가 없으면 기본값 'eventstorming_generator' 사용
        return os.getenv('NAMESPACE', 'eventstorming_generator')

    @staticmethod
    def get_pod_id() -> str:
        return os.getenv('POD_ID')


    @staticmethod
    def is_local_run() -> bool:
        return os.getenv('IS_LOCAL_RUN') == 'true'
    
    @staticmethod
    def is_use_generator_cache() -> bool:
        return os.getenv('USE_GENERATOR_CACHE', 'false') == 'true'
    

    @staticmethod
    def autoscaler_namespace() -> str:
        return os.getenv('AUTO_SCALE_NAMESPACE', 'default')
    
    @staticmethod
    def autoscaler_deployment_name() -> str:
        return os.getenv('AUTO_SCALE_DEPLOYMENT_NAME', 'eventstorming-generator')
    
    @staticmethod
    def autoscaler_service_name() -> str:
        return os.getenv('AUTO_SCALE_SERVICE_NAME', 'eventstorming-generator-service')

    @staticmethod
    def autoscaler_min_replicas() -> int:
        return int(os.getenv('AUTO_SCALE_MIN_REPLICAS', '1'))

    @staticmethod
    def autoscaler_max_replicas() -> int:
        return int(os.getenv('AUTO_SCALE_MAX_REPLICAS', '3'))
    
    @staticmethod
    def autoscaler_target_jobs_per_pod() -> int:
        return int(os.getenv('AUTO_SCALE_TARGET_JOBS_PER_POD', '1'))
    

    @staticmethod
    def get_log_level() -> str:
        """환경별 로그 레벨 반환 (DEBUG, INFO, WARNING, ERROR)"""
        if Config.is_local_run():
            return os.getenv('LOG_LEVEL', 'DEBUG')  # 로컬에서는 DEBUG 기본
        else:
            return os.getenv('LOG_LEVEL', 'INFO')   # Pod에서는 INFO 기본
    

    @staticmethod
    def get_ai_model() -> str:
        return os.getenv('AI_MODEL')
    
    @staticmethod
    def get_ai_model_vendor() -> str:
        vendor, _ = Config._split_model_string(Config.get_ai_model())
        return vendor
    
    @staticmethod
    def get_ai_model_name() -> str:
        _, name = Config._split_model_string(Config.get_ai_model())
        return name
    
    @staticmethod
    def get_ai_model_max_input_limit() -> int:
        return int(os.getenv('AI_MODEL_MAX_INPUT_LIMIT'))
    
    @staticmethod
    def get_ai_model_max_batch_size() -> int:
        return int(os.getenv('AI_MODEL_MAX_BATCH_SIZE'))
    

    @staticmethod
    def get_ai_model_light() -> str:
        return os.getenv('AI_MODEL_LIGHT')
    
    @staticmethod
    def get_ai_model_light_vendor() -> str:
        vendor, _ = Config._split_model_string(Config.get_ai_model_light())
        return vendor
    
    @staticmethod
    def get_ai_model_light_name() -> str:
        _, name = Config._split_model_string(Config.get_ai_model_light())
        return name

    @staticmethod
    def get_ai_model_light_max_input_limit() -> int:
        return int(os.getenv('AI_MODEL_LIGHT_MAX_INPUT_LIMIT'))
    
    @staticmethod
    def get_ai_model_light_max_batch_size() -> int:
        return int(os.getenv('AI_MODEL_LIGHT_MAX_BATCH_SIZE'))
    

    @staticmethod
    def get_msaez_es_url(dbuid: str) -> str:
        return f"{Config.get_msaez_url()}/#/storming/{dbuid}"

    @staticmethod
    def get_msaez_url() -> str:
        return os.getenv('MSAEZ_URL')


    @staticmethod
    def get_firebase_service_account_path() -> str:
        return os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH')
    
    @staticmethod
    def get_firebase_database_url() -> str:
        return os.getenv('FIREBASE_DATABASE_URL')

    @staticmethod
    def get_db_type() -> str:
        return os.getenv('DB_TYPE', 'firebase')
    
    @staticmethod
    def set_db_type(db_type: str):
        os.environ['DB_TYPE'] = db_type


    @staticmethod
    def get_text_chunker_chunk_size() -> int:
        return int(os.getenv('TEXT_CHUNKER_CHUNK_SIZE', '25000'))
    
    @staticmethod
    def get_text_chunker_spare_size() -> int:
        return int(os.getenv('TEXT_CHUNKER_SPARE_SIZE', '2000'))
    

    @staticmethod
    def get_llm_cache_path() -> str:
        return os.getenv('LLM_CACHE_PATH', '.cache/llm_cache.db')
    
    @staticmethod
    def set_llm_cache_path(path: str):
        os.environ['LLM_CACHE_PATH'] = path
    

    @staticmethod
    def a2a_external_url() -> str:
        return os.getenv('A2A_EXTERNAL_URL', 'http://localhost:5000')