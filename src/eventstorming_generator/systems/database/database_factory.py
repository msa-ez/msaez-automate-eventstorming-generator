import os
from typing import Dict

from .database_system import DatabaseSystem
from .firebase_system import FirebaseSystem
from .memory_db_system import MemoryDBSystem
from .acebase_system import AceBaseSystem
from .postgres_system import PostgresSystem
from ...config import Config
from ...utils.logging_util import LoggingUtil

class DatabaseFactory:
    _db_system_instance: Dict[str, DatabaseSystem] = {}

    @staticmethod
    def get_db_system() -> DatabaseSystem:
        """
        애플리케이션 환경에 맞는 DB 시스템 인스턴스를 반환합니다.
        싱글톤으로 동작하여 최초 호출 시 인스턴스를 생성하고 캐싱합니다.
        """
        db_type = Config.get_db_type()
        if db_type in DatabaseFactory._db_system_instance:
            return DatabaseFactory._db_system_instance[db_type]
        
        if db_type == "memory":
            DatabaseFactory._db_system_instance[db_type] = MemoryDBSystem()
        elif db_type == "firebase":
            DatabaseFactory._db_system_instance[db_type] = FirebaseSystem.initialize(
                service_account_path=Config.get_firebase_service_account_path(),
                database_url=Config.get_firebase_database_url()
            )
        elif db_type == "acebase":
            # AceBase 초기화
            host = os.getenv('ACEBASE_HOST', '127.0.0.1')
            port = int(os.getenv('ACEBASE_PORT', '5757'))
            dbname = os.getenv('ACEBASE_DB_NAME', 'mydb')
            https = os.getenv('ACEBASE_HTTPS', 'false').lower() == 'true'
            # 인증은 선택적: 환경 변수가 설정된 경우에만 인증 시도
            username = os.getenv('ACEBASE_USERNAME', None)
            password = os.getenv('ACEBASE_PASSWORD', None)
            
            LoggingUtil.info("database_factory", f"AceBase 시스템 초기화: {host}:{port}/{dbname}")
            if username and password:
                LoggingUtil.info("database_factory", f"AceBase 인증 정보 제공됨: {username}")
            else:
                LoggingUtil.info("database_factory", "AceBase 인증 정보 없음: 인증 없이 진행")
            
            DatabaseFactory._db_system_instance[db_type] = AceBaseSystem.initialize(
                host=host,
                port=port,
                dbname=dbname,
                https=https,
                username=username,
                password=password
            )
        elif db_type == "postgres":
            # PostgreSQL 초기화 (v1.0.30 — DB-migration-plan.md §5)
            host = os.getenv('POSTGRES_HOST', '127.0.0.1')
            port = int(os.getenv('POSTGRES_PORT', '5432'))
            dbname = os.getenv('POSTGRES_DB', 'msaez')
            user = os.getenv('POSTGRES_USER', 'msaez')
            password = os.getenv('POSTGRES_PASSWORD', '')

            LoggingUtil.info("database_factory", f"PostgreSQL 시스템 초기화: {host}:{port}/{dbname}")

            DatabaseFactory._db_system_instance[db_type] = PostgresSystem.initialize(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
        else:
            raise RuntimeError(f"Invalid database system type: {db_type}")
        
        return DatabaseFactory._db_system_instance[db_type]