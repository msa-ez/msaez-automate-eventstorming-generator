import os
import json
from typing import Dict, List, Any, Optional, Union, Type
from abc import ABC, abstractmethod

from langchain.chat_models import init_chat_model
from langchain.schema import HumanMessage, SystemMessage, AIMessage, BaseMessage
from langchain_community.cache import SQLiteCache
from langchain_core.globals import set_llm_cache
from langchain_core.runnables import RunnableConfig

from ..models import BaseModelWithItem
from ..utils import JsonUtil
from ..config import Config


def init_cache():
    if not os.path.exists(".cache"):
        os.makedirs(".cache")

    set_llm_cache(SQLiteCache(database_path=Config.get_llm_cache_path()))

if Config.is_use_generator_cache():
    init_cache()


class XmlBaseGenerator(ABC):
    """
    프롬프트 구성 및 LLM 호출을 위한 기본 생성기 클래스
    
    이 클래스는 구조화된 프롬프트 생성 및 LangChain 모델과의 통합을 위한 인터페이스를 제공합니다.
    상속받는 클래스는 프롬프트 구성 요소들을 구현하고, 이 베이스 클래스는 이를 조합하여 
    일관된 프롬프트 형식을 제공합니다.
    """
    
    _model_cache: Dict[str, Any] = {}
    _structured_model_cache: Dict[str, Any] = {}
    
    def __init__(self, model_name: str, structured_output_class: Type, model_kwargs: Optional[Dict[str, Any]] = None, client: Optional[Dict[str, Any]] = None):
        """
        XmlBaseGenerator 초기화
        
        Args:
            model_name: 모델 이름
            model_kwargs: 모델 파라미터
            client: 클라이언트
            structured_output_class: 구조화된 출력을 위한 Pydantic 모델 클래스
        """
        if not model_name or not structured_output_class:
            raise ValueError("model_name and structured_output_class are required")
        
        if model_kwargs is None: model_kwargs = {}
        if model_kwargs.get("temperature") is None:
            if "gpt-4.1" in model_name:
                model_kwargs["temperature"] = 0.3
            
            if "gemini" in model_name:
                model_kwargs["temperature"] = 0.3

                if model_name.endswith(":thinking"):
                    model_kwargs["include_thoughts"] = True
                    model_kwargs["thinking_budget"] = 8192
                    model_name = model_name.replace(":thinking", "")
                
                elif model_name.endswith(":no-thinking"):
                    model_kwargs["include_thoughts"] = False
                    model_kwargs["thinking_budget"] = 0
                    model_name = model_name.replace(":no-thinking", "")
            
            if model_kwargs["temperature"]:
                # 재시도 횟수에 따른 적응형 온도 조절
                model_kwargs["temperature"] = min(
                    model_kwargs.get("temperature") + client.get("retryCount", 0) * 0.2, 1.0
                )
        
        if client is None: client = {}
        if not client.get("inputs"): client["inputs"] = {}
        if not client.get("preferredLanguage"): client["preferredLanguage"] = "English"
        if not client.get("disableLanguageGuide"): client["disableLanguageGuide"] = False

        if self.inputs_types_to_check:
            for input_type in self.inputs_types_to_check:
                if client.get("inputs").get(input_type) == None:
                    raise ValueError(f"{input_type} is required")

        self.structured_output_class = structured_output_class
        self.client = client
        self.set_model(model_name, model_kwargs)

    def assemble_prompt(self) -> Dict[str, Union[str, List[str]]]:
        """
        시스템, 유저, 어시스턴트 프롬프트를 조합하여 완전한 프롬프트 구조 반환
        
        Returns:
            Dict: 각 역할별 프롬프트가 포함된 딕셔너리
        """
        return {
            "system": self._build_system_prompt(),
            "user": self._build_user_prompt(),
            "assistant": self._build_assistant_prompt()
        }
    
    def _build_system_prompt(self) -> str:
        """시스템 프롬프트 빌드"""
        persona_info = self._build_persona_info()
        if persona_info["persona"] and persona_info["goal"] and persona_info["backstory"]:
            return f"""<persona_and_role>
    <persona>{persona_info["persona"]}</persona>
    <goal>{persona_info["goal"]}</goal>
    <backstory>{persona_info["backstory"]}</backstory>
</persona_and_role>"""
        else:
            return ""
    
    def _build_user_prompt(self) -> List[str]:
        """유저 프롬프트 빌드"""
        prompts = []
        
        instruction_prompt = self._build_task_instruction_prompt()
        example_inputs = self._build_json_example_input_format()
        user_inputs = self._build_json_user_query_input_format()
        
        approve_request = "<request>This is the entire guideline. When you're ready, please output 'Approved.' Then I will begin user input.</request>"
        if example_inputs and user_inputs:
            prompts.append(instruction_prompt + "\n\n" + approve_request)
            prompts.append(self._inputs_to_string(example_inputs))
            prompts.append(self._inputs_to_string(user_inputs))
        elif user_inputs:
            prompts.append(instruction_prompt + "\n\n" + approve_request)
            prompts.append(self._inputs_to_string(user_inputs))
        
        return prompts
    
    def _build_assistant_prompt(self) -> List[str]:
        """어시스턴트 프롬프트 빌드"""
        example_outputs = self._build_json_example_output_format()
        if not example_outputs:
            return []
        
        return ["Approved.", JsonUtil.convert_to_json(example_outputs, 4)]
    
    def _inputs_to_string(self, inputs: Dict[str, Any]) -> str:
        """입력 파라미터를 문자열로 변환"""
        result = []
        
        for key, value in inputs.items():
            formatted_value = value if isinstance(value, str) else JsonUtil.convert_to_json(value, 0)
            result.append(f"<{key.strip()}>{formatted_value.strip()}</{key.strip()}>")
            
        return "<inputs>\n" + "\n".join(result) + "\n</inputs>"
    
    def generate(self, bypass_cache: bool = False, retry_count: int = 0, extra_config_metadata: Dict[str, Any] = {}) -> Any:
        """
        LLM을 사용하여 생성 실행
        
        Args:
            bypass_cache: 캐시 우회 여부
            
        Returns:
            생성된 결과 (구조화된 출력이 설정된 경우 해당 클래스의 인스턴스)
        """
        if not self.model:
            raise ValueError("모델이 설정되지 않았습니다. 생성기를 초기화할 때 model 파라미터를 전달하거나 set_model()을 호출하세요.")
        if not Config.is_local_run():
            bypass_cache = False

        messages = self._get_messages(bypass_cache, retry_count)
        class_name = self.__class__.__name__ 

        structured_model = None
        if class_name in self._structured_model_cache and \
           self._structured_model_cache[class_name] is not None:
            structured_model = self._structured_model_cache[class_name]
        else:
            structured_model = self.model.with_structured_output(
                self.structured_output_class,
                method="json_mode"
            )
            self._structured_model_cache[class_name] = structured_model

        config_metadata = {
            "generator_class": class_name,
            "retry_count": retry_count
        }
        if extra_config_metadata:
            config_metadata.update(extra_config_metadata)
        config = RunnableConfig(
            metadata=config_metadata
        )

        model_with_json_mode = structured_model.first
        raw_response = model_with_json_mode.invoke(messages, config=config)

        thinking = ""
        if self.__isThinkingAttributeExist(raw_response):
            thinking = raw_response.content[0]['thinking']

        parser = structured_model.last
        result = parser.invoke(raw_response)
        result = self._post_process_to_structured_output(result)
        return {
            "result": result,
            "thinking": thinking
        }
    def __isThinkingAttributeExist(self, raw_response: BaseMessage) -> bool:
        return hasattr(raw_response, 'content') and \
             type(raw_response.content) == list and \
             len(raw_response.content) > 0 and \
             type(raw_response.content[0]) == dict and \
             raw_response.content[0]['type'] == 'thinking' and \
             raw_response.content[0]['thinking']

    def _post_process_to_structured_output(self, structured_output: BaseModelWithItem) -> BaseModelWithItem:
        return structured_output

    def _get_messages(self, bypass_cache: bool = False, retry_count: int = 0) -> List[BaseMessage]:
        promptsToBuild = self._get_prompts_to_build()

        messages = []
        
        if promptsToBuild["system"]:
            system_content = promptsToBuild["system"]
            if bypass_cache:
                system_content += f"<cache_bypass retry_count=\"{retry_count}\"/>"
            messages.append(SystemMessage(content=system_content))

        for i in range(len(promptsToBuild["user"])):
            messages.append(HumanMessage(content=promptsToBuild["user"][i]))
            if(i < len(promptsToBuild["assistant"])):
                messages.append(AIMessage(content=promptsToBuild["assistant"][i]))
        
        return messages

    def _get_prompts_to_build(self) -> Dict[str, Union[str, List[str]]]:
        promptsToBuild = {
            "system": "",
            "user": [],
            "assistant": []
        }

        createPromptWithRoles = self.assemble_prompt()
        promptsToBuild["system"] = createPromptWithRoles["system"]
        promptsToBuild["user"] = createPromptWithRoles["user"]
        if(promptsToBuild["user"] and len(promptsToBuild["user"]) > 0 and not self.client.get("disableLanguageGuide")):
            promptsToBuild["user"][len(promptsToBuild["user"]) - 1] += "\n<language_guide>Please generate the response in " + self.client.get("preferredLanguage") + " while ensuring that all code elements (e.g., variable names, function names) remain in English.</language_guide>"
        
        promptsToBuild["assistant"] = createPromptWithRoles["assistant"]

        return promptsToBuild
    
    def set_model(self, model_name: str, model_kwargs: Optional[Dict[str, Any]] = None) -> None:
        """
        LangChain 모델 설정 (캐싱 지원)
        
        Args:
            model_name: 모델 이름
            model_kwargs: 모델 파라미터
        """
        if model_kwargs is None: model_kwargs = {}
        
        # 캐시 키 생성
        cache_key = self._get_cache_key(model_name, model_kwargs)
        
        # 캐시에서 모델 확인
        if cache_key in self._model_cache:
            self.model = self._model_cache[cache_key]
        else:
            # 새 모델 생성 및 캐시에 저장
            init_kwargs = model_kwargs.copy()

            if model_name.startswith("google_genai") and not Config.is_local_run():
                init_kwargs["google_api_key"] = os.getenv("GOOGLE_API_KEY")

            if model_name.startswith("openai"):
                base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
                if base_url:
                    init_kwargs.setdefault("base_url", base_url)
                api_key = os.getenv("OPENAI_API_KEY")
                if api_key:
                    init_kwargs.setdefault("api_key", api_key)

            self.model = init_chat_model(model_name, **init_kwargs)
            self._model_cache[cache_key] = self.model
    
    def _get_cache_key(self, model_name: str, model_kwargs: Dict[str, Any]) -> str:
        """
        모델 캐시 키 생성
        
        Args:
            model_name: 모델 이름
            model_kwargs: 모델 파라미터
            
        Returns:
            str: 캐시 키
        """
        # model_kwargs를 정렬된 JSON 문자열로 변환하여 일관된 키 생성
        sorted_kwargs = json.dumps(model_kwargs, sort_keys=True, ensure_ascii=False)
        return f"{model_name}:{sorted_kwargs}"

    @classmethod
    def clear_model_cache(cls) -> None:
        """
        모델 캐시 전체 삭제
        """
        cls._model_cache.clear()
    
    @classmethod
    def get_cache_size(cls) -> int:
        """
        현재 캐시된 모델 개수 반환
        
        Returns:
            int: 캐시된 모델 개수
        """
        return len(cls._model_cache)
    
    @classmethod
    def get_cached_model_keys(cls) -> List[str]:
        """
        캐시된 모델 키 목록 반환
        
        Returns:
            List[str]: 캐시된 모델 키 목록
        """
        return list(cls._model_cache.keys())
    
    def get_token_count(self) -> int:
        """
        현재 구축된 메세지들의 전체 토큰 수 반환
        """
        messages = self._get_messages()

        total_contents = ""
        for message in messages:
            total_contents += message.content
        
        return self.model.get_num_tokens(total_contents)
    
    def get_entire_prompt(self) -> str:
        """
        현재 구축된 메세지들의 전체 프롬프트 반환
        """
        messages = self._get_messages()
        return "\n---------\n".join([message.content for message in messages])
    
    # 아래 메서드들은 상속 클래스에서 구현해야 함
    
    @abstractmethod
    def _build_persona_info(self) -> Dict[str, str]:
        """
        AI 에이전트의 역할 및 전문 분야 정의
        
        Returns:
            str: 에이전트 역할 프롬프트
        """
        return {
            "persona": "",
            "goal": "",
            "backstory": ""
        }
    
    @abstractmethod
    def _build_task_instruction_prompt(self) -> str:
        """
        작업 수행을 위한 가이드라인 정의
        
        Returns:
            str: 작업 가이드라인 프롬프트
        """
        return ""
    
    def _build_json_example_input_format(self) -> Optional[Dict[str, Any]]:
        """
        예제 입력 형식 정의 (선택적 구현)
        
        Returns:
            Optional[Dict]: 예제 입력 형식
        """
        return None
    
    def _build_json_user_query_input_format(self) -> Dict[str, Any]:
        """
        사용자 쿼리 입력 형식 정의 (선택적 구현)
        
        Returns:
            Dict: 사용자 쿼리 입력 형식
        """
        return {}
    
    def _build_json_example_output_format(self) -> Optional[Dict[str, Any]]:
        """
        예제 출력 형식 정의 (선택적 구현)
        
        Returns:
            Optional[Dict]: 예제 출력 형식
        """
        return None