from ....utils import TokenCounter, LoggingUtil
from ...terminal_helper import TerminalHelper

def run_token_counter(command_args):
    run_name = "run_token_counter"

    try:

        token_count = TokenCounter.get_token_count("Hello, world!", "openai", "gpt-4.1-2025-04-14")
        LoggingUtil.info(run_name, f"토큰 수: {token_count}")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise