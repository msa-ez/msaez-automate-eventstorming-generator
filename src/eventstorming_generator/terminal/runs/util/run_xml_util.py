from ...terminal_helper import TerminalHelper
from ....utils import LoggingUtil, XmlUtil
from ..mocks import xml_util_inputs

def run_xml_util(command_args):
    run_name = "run_xml_util"

    try:

        xml_string = XmlUtil.from_dict(xml_util_inputs, "root")
        TerminalHelper.save_dict_to_temp_file(xml_string, f"{run_name}_xml")

    except Exception as e:
        LoggingUtil.exception(run_name, f"실행 실패", e)
        TerminalHelper.save_dict_to_temp_file({
            "error": str(e)
        }, f"{run_name}_error")
        raise