import os
import time
from datetime import datetime
from typing import Any

from ..generators import XmlBaseGenerator
from ..utils import JsonUtil, LoggingUtil
from ..config import Config

class TerminalHelper:
    @staticmethod
    def save_dict_to_temp_file(data: Any, file_name: str, directory: str = ".temp") -> None:
        os.makedirs(directory, exist_ok=True)

        json_data = JsonUtil.convert_to_json(data)  
        
        TIME = datetime.now().strftime('%Y%m%d_%H%M%S')
        FILE_PATH = f"{directory}/{TIME}_{file_name}.json"
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            f.write(json_data)
        print(f"{FILE_PATH} 파일에 생성 결과 저장 완료")   

    @staticmethod
    def run_generator(generator: XmlBaseGenerator, inputs: dict, model_type: str = "normal", is_save_to_temp: bool = True) -> Any:
        try:

            model_name = Config.get_ai_model()
            if model_type == "light":
                model_name = Config.get_ai_model_light()

            generator = generator(model_name, {}, {
                "preferredLanguage": "Korean",
                "inputs": inputs
            })
            entire_prompt = generator.get_entire_prompt()


            start_time = time.time()
            generator_output = generator.generate()


            if is_save_to_temp:
                end_time = time.time()
                total_seconds = end_time - start_time

                temp_output = {
                    "result": generator_output["result"].model_dump(),
                    "total_seconds": total_seconds
                }
                TerminalHelper.save_dict_to_temp_file(entire_prompt, f"run_input_{generator.__class__.__name__}")
                TerminalHelper.save_dict_to_temp_file(temp_output, f"run_output_{generator.__class__.__name__}")
 
            return generator_output["result"]

        except Exception as e:
            LoggingUtil.exception(f"run_error_{generator.__class__.__name__}", f"실행 실패", e)
            TerminalHelper.save_dict_to_temp_file({
                "error": str(e)
            }, f"run_error_{generator.__class__.__name__}")
            raise