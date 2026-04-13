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
        
        Args:
            text: 토큰 수를 계산할 텍스트
            model_name: 토큰 계산에 사용할 모델 이름
            
        Returns:
            계산된 토큰 수
        """
        # 모델에 따른 인코더 선택
        model = init_chat_model(f"{model_vendor}:{model_name}", **_build_init_kwargs(model_vendor))
        return model.get_num_tokens(text)
    
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