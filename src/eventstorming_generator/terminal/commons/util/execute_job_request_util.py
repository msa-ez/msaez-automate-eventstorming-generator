import time

from ....utils import LoggingUtil
from ....utils.job_utils import JobRequestUtil
from ..mocks import common_requirements
from ...terminal_helper import TerminalHelper

async def execute_job_request_util(job_type:str="library_requirements"):
    DIR_PATH = f".temp/{time.time()}_execute_job_request_util"
    requirements = common_requirements.get(job_type)

    try:
        index = 0
        async for event in JobRequestUtil.add_job_request_with_streaming(requirements):
            print(event)
            TerminalHelper.save_dict_to_temp_file(event, f"event_{index}.json", DIR_PATH)
            index += 1
    
    except Exception as e:
        LoggingUtil.exception(f"execute_job_request_util", f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"error.json", DIR_PATH)