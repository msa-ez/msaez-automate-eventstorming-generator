import os
import re
import json
import time
import concurrent.futures
from typing import Dict, List, Any, Optional, Union, Type
from abc import ABC, abstractmethod

from langchain.chat_models import init_chat_model
from langchain.schema import HumanMessage, SystemMessage, AIMessage, BaseMessage
from langchain_community.cache import SQLiteCache
from langchain_core.globals import set_llm_cache
from langchain_core.runnables import RunnableConfig

from ..models import BaseModelWithItem
from ..utils import JsonUtil, LoggingUtil
from ..config import Config
from eventstorming_generator.utils.catchable_exceptions import CATCHABLE_EXCEPTIONS


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

        config_metadata = {
            "generator_class": class_name,
            "retry_count": retry_count
        }
        if extra_config_metadata:
            config_metadata.update(extra_config_metadata)
        config = RunnableConfig(
            metadata=config_metadata
        )

        # NOTE: 이전엔 self.model.with_structured_output(method="json_mode") 를 사용해
        # response_format={"type":"json_object"} 가 매 요청에 강제 부착되었다.
        # OpenAI 호환 사내 프록시(P-GPT 등)가 response_format 을 미지원/무시할 경우
        # 무한 hang 또는 텍스트로 응답이 와서 파서 실패가 반복되는 문제가 있어,
        # plain invoke 로 받아 후처리 파싱하는 방식으로 회귀한다.
        # _build_assistant_prompt 가 JSON 예시 출력을 어시스턴트 메시지로 주입하므로
        # 모델은 표준 chat completions 만으로도 JSON 응답을 안정적으로 생성한다.
        raw_response = self._invoke_with_observability(messages, config, class_name, retry_count)

        thinking = ""
        if self.__isThinkingAttributeExist(raw_response):
            thinking = raw_response.content[0]['thinking']

        result = self._parse_response_to_structured_output(raw_response)
        result = self._post_process_to_structured_output(result)
        return {
            "result": result,
            "thinking": thinking
        }

    def _invoke_with_observability(
        self,
        messages: List[BaseMessage],
        config: RunnableConfig,
        class_name: str,
        retry_count: int,
    ) -> BaseMessage:
        """LLM invoke 를 stdout 로깅 + 하드 타임아웃으로 감싼 래퍼.

        - langchain 의 ``timeout`` 은 provider 마다 적용 범위가 달라 (특히 google_genai
          처럼 무시되는 경우, 또는 streaming 경로에서 read timeout 이 안 걸리는 경우)
          실제로는 무한 대기로 빠지는 케이스가 관측됨. 외부에서 ThreadPoolExecutor 로
          한번 더 감싸 ``LLM_HARD_TIMEOUT_SEC`` (기본: ``LLM_TIMEOUT_SEC * 1.5``) 안에
          반드시 예외가 나오도록 강제한다.
        - 이번 사이클에서 invoke 가 어디서 멈췄는지(어느 모델/어느 generator/몇 번째 retry)
          를 stdout 으로 즉시 식별 가능하도록 시작/종료/실패 시 LoggingUtil 호출.
          실패 시점은 hang 진단의 거의 유일한 단서이므로 elapsed/원인까지 같이 남긴다.
        """
        soft_timeout = float(os.getenv("LLM_TIMEOUT_SEC", "120"))
        hard_timeout_env = os.getenv("LLM_HARD_TIMEOUT_SEC")
        hard_timeout = float(hard_timeout_env) if hard_timeout_env else soft_timeout * 1.5

        prompt_chars = 0
        for m in messages:
            content = getattr(m, "content", "")
            if isinstance(content, str):
                prompt_chars += len(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        prompt_chars += len(str(part.get("text") or part.get("content") or ""))
                    else:
                        prompt_chars += len(str(part))

        LoggingUtil.info(
            "xml_base",
            f"LLM invoke start: model={self.model_name} generator={class_name} "
            f"retry={retry_count} prompt_chars={prompt_chars} hard_timeout={hard_timeout:.0f}s",
        )

        start = time.time()
        # ``cancel_futures=True`` 는 미실행 작업만 취소하므로 이미 실행 중인 invoke
        # 스레드는 살아남는다. 이는 의도적: provider 라이브러리가 비협조적이어도
        # 호출 측은 즉시 timeout 으로 빠져 다음 retry 로 갈 수 있게 한다 (orphan
        # 스레드는 subprocess 종료 시 함께 정리됨).
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="llm-invoke"
        )
        try:
            future = executor.submit(self.model.invoke, messages, config=config)
            try:
                raw_response = future.result(timeout=hard_timeout)
            except concurrent.futures.TimeoutError:
                elapsed = time.time() - start
                LoggingUtil.warning(
                    "xml_base",
                    f"LLM invoke HARD TIMEOUT: model={self.model_name} "
                    f"generator={class_name} retry={retry_count} "
                    f"elapsed={elapsed:.1f}s hard_timeout={hard_timeout:.0f}s — "
                    f"langchain timeout 이 적용되지 않은 provider/경로일 수 있음",
                )
                raise TimeoutError(
                    f"LLM invoke exceeded hard timeout {hard_timeout:.0f}s "
                    f"(model={self.model_name}, generator={class_name})"
                )
            except Exception as e:
                elapsed = time.time() - start
                LoggingUtil.warning(
                    "xml_base",
                    f"LLM invoke failed: model={self.model_name} generator={class_name} "
                    f"retry={retry_count} elapsed={elapsed:.1f}s error={e!r}",
                )
                raise
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        elapsed = time.time() - start
        LoggingUtil.info(
            "xml_base",
            f"LLM invoke done: model={self.model_name} generator={class_name} "
            f"retry={retry_count} elapsed={elapsed:.1f}s",
        )
        return raw_response

    def _parse_response_to_structured_output(self, raw_response: BaseMessage) -> Any:
        """LLM 응답에서 JSON 본문을 추출하여 structured_output_class 로 검증한다.

        - thinking 모델: content 가 list ([{type:'thinking',...},{type:'text',text:'...'}]) → text 블록만 합침
        - 일반 모델: content 가 str
        - 어느 경우든 ```json``` 코드펜스 / 앞뒤 자연어 설명을 제거한다.
        """
        text = self._extract_text_from_response_content(raw_response.content)
        json_str = self._extract_json_payload(text)
        return self.structured_output_class.model_validate_json(json_str)

    @staticmethod
    def _extract_text_from_response_content(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: List[str] = []
            for block in content:
                if isinstance(block, dict):
                    # thinking 블록은 본문 아님
                    if block.get("type") == "thinking":
                        continue
                    if "text" in block:
                        parts.append(str(block["text"]))
                    elif "content" in block:
                        parts.append(str(block["content"]))
                else:
                    parts.append(str(block))
            return "\n".join(parts)
        return str(content)

    @staticmethod
    def _extract_json_payload(text: str) -> str:
        """평문에서 JSON 본문을 추출. 코드펜스 → JSON 시작/끝 매칭 순으로 시도."""
        if not text:
            return text
        text = text.strip()

        # 1) ```json ... ``` 또는 ``` ... ``` 코드펜스 우선
        fence = re.search(r"```(?:json)?\s*\n?(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
        if fence:
            return fence.group(1).strip()

        # 2) 첫 { 또는 [ 부터 마지막 매칭 } 또는 ] 까지 잘라낸다
        first_obj = text.find("{")
        first_arr = text.find("[")
        candidates = [p for p in (first_obj, first_arr) if p >= 0]
        if not candidates:
            return text  # JSON 단서 없음 — 검증 단계에서 실패시켜 retry 로 위임

        start = min(candidates)
        last_obj = text.rfind("}")
        last_arr = text.rfind("]")
        end = max(last_obj, last_arr)
        if end <= start:
            return text[start:]
        return text[start:end + 1]
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

        # invoke 로그/타임아웃 메시지에서 어떤 모델이 hang 했는지 식별하기 위해 보관
        self.model_name = model_name

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

            # 기본 타임아웃 — 사내 프록시(P-GPT 등) 가 silent hang 되는 경우에도
            # 일정 시간 안에 예외가 발생해 외부 retry 로직(retryCount 기반 temperature 상승)
            # 으로 흐르도록 한다. 환경변수 LLM_TIMEOUT_SEC 로 덮어쓸 수 있다.
            timeout_sec = float(os.getenv("LLM_TIMEOUT_SEC", "120"))
            init_kwargs.setdefault("timeout", timeout_sec)

            # 출력 토큰 상한 — 미지정 시 OpenAI 가 응답 generation 단계에서 hang 되는
            # 케이스(특히 큰 aggregate 의 다수 command/event/readModel 출력)를 회피.
            # 32768 은 gpt-4.1-mini 의 max_completion_tokens 한도와 동일하므로 정상
            # 응답이 잘릴 위험은 사실상 0. 환경변수 LLM_MAX_TOKENS 로 덮어쓸 수 있다.
            max_tokens_env = os.getenv("LLM_MAX_TOKENS")
            max_tokens = int(max_tokens_env) if max_tokens_env else 32768
            init_kwargs.setdefault("max_tokens", max_tokens)

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
        현재 구축된 메세지들의 전체 토큰 수 반환.

        주의: langchain-openai 의 ``get_num_tokens`` 는 내부적으로 tiktoken 을 통해
        인코딩 파일(`o200k_base.tiktoken` 등) 을 ``openaipublic.blob.core.windows.net``
        에서 다운로드하려 시도한다. 폐쇄망 환경에서 외부 도달이 불가하면 ConnectTimeout
        으로 worker 가 무한 재시도되며 hang 처럼 보이는 증상이 발생한다. 이미지에 미리
        캐시되어 있어도 어떤 이유로든 캐시 미스 시 같은 문제가 재현될 수 있어, 네트워크
        오류가 발생하면 글자 수 기반 추정치로 폴백하고 worker 흐름이 끊기지 않게 한다.
        (대략 영문 4자/토큰 기준; 다국어/한글 혼재 시 안전하게 3자/토큰 으로 잡음)
        """
        messages = self._get_messages()

        total_contents = ""
        for message in messages:
            total_contents += message.content

        try:
            return self.model.get_num_tokens(total_contents)
        except CATCHABLE_EXCEPTIONS as e:
            LoggingUtil.warning(
                "xml_base",
                f"get_num_tokens 실패 — 글자 수 기반 추정치로 폴백합니다 (원인: {e!r})",
            )
            # 폐쇄망 등에서 tiktoken 캐시가 없을 때 안전 폴백
            return max(1, len(total_contents) // 3)
    
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