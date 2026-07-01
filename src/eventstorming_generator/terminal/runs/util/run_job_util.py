from ....utils import LoggingUtil, ListUtil
from ....utils.job_utils import JobUtil

run_name = "run_job_util"
def run_job_util(command_args):
    func_name = ListUtil.get_safely(command_args, 1, None)
    func_dic = {
        "createJobId": run_create_job_id,
    }
    func = func_dic.get(func_name, None)
    if not func:
        LoggingUtil.error(run_name, f"유효하지 않은 함수 이름: {func_name}")
        return False
    
    return func(command_args)

def run_create_job_id(command_args):
    job_id = JobUtil.create_job_id()
    LoggingUtil.info(run_name, f"새로운 Job ID: {job_id}")

    is_valid = JobUtil.is_valid_job_id(job_id)
    if not is_valid:
        raise RuntimeError(f"유효하지 않은 Job ID: {job_id}")

    return True