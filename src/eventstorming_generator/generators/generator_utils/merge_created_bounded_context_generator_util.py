from typing import List, Set

from ...models import BoundedContextInfoModel, MergeCreatedBoundedContextGeneratorOutput
from ...generators import MergeCreatedBoundedContextGenerator

class MergeCreatedBoundedContextGeneratorUtil:
    @staticmethod
    def merge_created_bounded_context_safely(
        bounded_context_infos: List[BoundedContextInfoModel], 
        model_name: str,
        preferred_language: str,
        max_retry_count: int,
        job_id: str
    ) -> List[BoundedContextInfoModel]:
        """
        생성된 Bounded Context 정보들을 안전하게 병합합니다.
        중복 제거, 유효성 검증, 자동 재시도 기능을 포함합니다.
        
        Args:
            bounded_context_infos: 병합할 BoundedContextInfoModel 리스트
            model_name: 사용할 LLM 모델 이름
            preferred_language: 선호 언어
            max_retry_count: 최대 재시도 횟수
            
        Returns:
            병합된 BoundedContextInfoModel 리스트
        """
        # 입력 데이터가 비어있는 경우
        if not bounded_context_infos:
            return []
        
        # 이미 중복이 없는 경우 (3개 이하인 경우 병합 불필요)
        if len(bounded_context_infos) <= 3:
            return MergeCreatedBoundedContextGeneratorUtil._deduplicate_bounded_contexts(
                bounded_context_infos
            )
        
        for retry_count in range(max_retry_count):
            try:
                # MergeCreatedBoundedContextGenerator 초기화
                client = {
                    "inputs": {
                        "boundedContexts": [bc.model_dump() for bc in bounded_context_infos]
                    },
                    "preferredLanguage": preferred_language,
                    "retryCount": retry_count
                }
                
                generator = MergeCreatedBoundedContextGenerator(
                    model_name=model_name,
                    client=client
                )
                
                # 생성 실행
                result = generator.generate(
                    bypass_cache=(retry_count > 0),
                    retry_count=retry_count,
                    extra_config_metadata={
                        "job_id": job_id
                    }
                )
                output: MergeCreatedBoundedContextGeneratorOutput = result.get("result")
                
                # 데이터 정합성 검증 및 중복 제거
                validated_output = MergeCreatedBoundedContextGeneratorUtil._validate_and_deduplicate(
                    output,
                    bounded_context_infos
                )
                
                # 최종 검증
                if MergeCreatedBoundedContextGeneratorUtil._is_valid_bounded_contexts(validated_output):
                    return validated_output
                else:
                    raise ValueError("Generated bounded contexts failed final validation")
                    
            except Exception as e:
                if retry_count == max_retry_count - 1:
                    # 최종 재시도 실패 시, 원본 데이터에서 중복만 제거하여 반환
                    return MergeCreatedBoundedContextGeneratorUtil._deduplicate_bounded_contexts(
                        bounded_context_infos
                    )
        
        # 이 코드는 도달하지 않아야 하지만, 안전장치
        return MergeCreatedBoundedContextGeneratorUtil._deduplicate_bounded_contexts(
            bounded_context_infos
        )
    
    @staticmethod
    def _validate_and_deduplicate(
        output: MergeCreatedBoundedContextGeneratorOutput,
        original_contexts: List[BoundedContextInfoModel]
    ) -> List[BoundedContextInfoModel]:
        """
        생성된 출력의 중복을 제거하고 유효성을 검증합니다.
        
        Args:
            output: MergeCreatedBoundedContextGeneratorOutput
            original_contexts: 원본 BoundedContextInfoModel 리스트 (폴백용)
            
        Returns:
            검증 및 중복 제거된 BoundedContextInfoModel 리스트
        """
        if not output or not output.mergedBoundedContexts:
            raise ValueError("Output has no merged bounded contexts")
        
        # Bounded Context 중복 제거
        unique_contexts = MergeCreatedBoundedContextGeneratorUtil._deduplicate_bounded_contexts(
            output.mergedBoundedContexts
        )
        
        if not unique_contexts:
            raise ValueError("No valid bounded contexts after deduplication")
        
        # 병합 결과가 너무 적거나 많은 경우 검증
        if len(unique_contexts) < 1:
            raise ValueError("Too few bounded contexts after merging")
        
        # 병합 후 컨텍스트 수가 원본보다 많으면 비정상
        if len(unique_contexts) > len(original_contexts):
            raise ValueError("Merged contexts count exceeds original count")
        
        return unique_contexts
    
    @staticmethod
    def _deduplicate_bounded_contexts(
        contexts: List[BoundedContextInfoModel]
    ) -> List[BoundedContextInfoModel]:
        """
        BoundedContextInfoModel 리스트에서 중복을 제거합니다.
        name 기준으로 중복을 판단하며, 대소문자 무시 및 공백 제거 후 비교합니다.
        
        Args:
            contexts: BoundedContextInfoModel 리스트
            
        Returns:
            중복이 제거된 BoundedContextInfoModel 리스트
        """
        if not contexts:
            return []
        
        seen_names: Set[str] = set()
        unique_contexts = []
        
        for context in contexts:
            # 필수 필드 검증
            if not context.name or not context.alias or not context.description:
                continue
            
            # importance 검증
            if context.importance not in ["Core Domain", "Supporting Domain", "Generic Domain"]:
                continue
            
            # 이름 정규화 (공백 제거 및 대소문자 처리)
            normalized_name = context.name.strip().lower()
            
            # 중복 체크
            if normalized_name in seen_names:
                continue
            
            seen_names.add(normalized_name)
            unique_contexts.append(context)
        
        return unique_contexts
    
    @staticmethod
    def _is_valid_bounded_contexts(contexts: List[BoundedContextInfoModel]) -> bool:
        """
        BoundedContextInfoModel 리스트의 유효성을 검증합니다.
        
        Args:
            contexts: 검증할 BoundedContextInfoModel 리스트
            
        Returns:
            유효성 여부
        """
        # 최소 1개 이상의 컨텍스트 필요
        if not contexts or len(contexts) < 1:
            return False
        
        # 각 컨텍스트 검증
        for context in contexts:
            # 필수 필드 검증
            if not context.name or not context.alias or not context.description:
                return False
            
            # name이 영어 PascalCase인지 검증 (첫 글자 대문자, 공백 없음)
            if not context.name[0].isupper() or ' ' in context.name:
                return False
            
            # importance 값 검증
            if context.importance not in ["Core Domain", "Supporting Domain", "Generic Domain"]:
                return False
        
        # 중복 이름 체크
        names = [context.name.lower() for context in contexts]
        if len(names) != len(set(names)):
            return False
        
        return True