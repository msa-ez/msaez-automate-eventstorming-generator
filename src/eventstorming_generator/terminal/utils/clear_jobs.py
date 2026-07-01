from ...systems import DatabaseFactory
from ...utils import LoggingUtil
from ..terminal_helper import TerminalHelper
from ...config import Config

def clear_jobs(command_args):
    util_name = "clear_jobs"
    critical_namespaces = ["eventstorming_generator"]

    namespace = command_args[0]
    if namespace is None:
        raise ValueError("namespace is required")
    if namespace in critical_namespaces:
        raise ValueError(f"namespace {namespace} is critical and cannot be deleted")
    if namespace == "me":
        namespace = Config.get_namespace()

    try:

        Config.set_db_type("firebase")
        db_system = DatabaseFactory.get_db_system()
        db_system.delete_data(f"jobs/{namespace}")
        db_system.delete_data(f"jobStates/{namespace}")
        db_system.delete_data(f"requestedJobs/{namespace}")
        LoggingUtil.info(util_name, f"jobs, jobStates, requestedJobs 폴더를 모두 삭제 완료: {namespace}")

    except Exception as e:
        LoggingUtil.exception(util_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{util_name}_error")
        raise