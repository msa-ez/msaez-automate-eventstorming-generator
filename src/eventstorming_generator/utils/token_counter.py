import os

from langchain.chat_models import init_chat_model


def _build_init_kwargs(model_vendor: str) -> dict:
    init_kwargs: dict = {}
    if model_vendor == "openai":
        base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
        if base_url:
            init_kwargs["base_url"] = base_url
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            init_kwargs["api_key"] = api_key
    return init_kwargs


class TokenCounter:
    """
    토큰 수 계산 유틸리티 클래스
    """
    
    @staticmethod
    def get_token_count(text: str, model_vendor:str, model_name: str) -> int:
        """
        주어진, 텍스트의 토큰 수를 계산합니다.

        주의: ``model.get_num_tokens`` 는 내부적으로 tiktoken 인코딩 파일을
        ``openaipublic.blob.core.windows.net`` 에서 다운로드하려 시도한다.
        폐쇄망에서 외부 도달 불가 시 ConnectTimeout 으로 호출자가 재시도 hang
        되는 문제가 있어, 네트워크 오류 발생 시 글자 수 기반 추정치로 폴백한다.
        (xml_base.XmlBaseGenerator.get_token_count 와 같은 정책)

        Args:
            text: 토큰 수를 계산할 텍스트
            model_name: 토큰 계산에 사용할 모델 이름

        Returns:
            계산된 토큰 수
        """
        try:
            model = init_chat_model(f"{model_vendor}:{model_name}", **_build_init_kwargs(model_vendor))
            return model.get_num_tokens(text)
        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:
            try:
                from ..utils.logging_util import LoggingUtil
                LoggingUtil.warning(
                    "token_counter",
                    f"get_num_tokens 실패 — 글자 수 기반 추정치로 폴백합니다 (원인: {e!r})",
                )
            except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as _exc:
                LoggingUtil.warning("token_counter", f"예외 발생(무시됨): {_exc}")
            return max(1, len(text) // 3)
    
    @staticmethod
    def is_within_token_limit(text: str, model_vendor:str, model_name: str, max_tokens: int) -> bool:
        """
        주어진 텍스트가 토큰 제한 내에 있는지 확인합니다.
        
        Args:
            text: 확인할 텍스트
            model_name: 토큰 계산에 사용할 모델 이름
            max_tokens: 최대 토큰 수
            
        Returns:
            토큰 제한 내에 있으면 True, 아니면 False
        """
        token_count = TokenCounter.get_token_count(text, model_vendor, model_name)
        return token_count <= max_tokens 