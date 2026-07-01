from typing import List, Set

from ...models import BoundedContextStructureModel, BoundedContextInfoModel, CreateDraftGeneratorOutput, AggregateInfoModel, EnumerationInfoModel, ValueObjectInfoModel, ValueObjectInfoNoRefModel
from ...generators import CreateDraftGenerator


class CreateDraftGeneratorUtil:
    @staticmethod
    def create_draft_by_function_safely(
        bounded_context_info: BoundedContextInfoModel,
        requirements: str,
        model_name: str,
        preferred_language: str,
        max_retry_count: int,
        job_id: str
    ) -> BoundedContextStructureModel:
        """
        바운디드 컨텍스트 정보와 요구사항을 받아 드래프트를 생성합니다.
        
        Args:
            bounded_context_info: BoundedContextInfoModel
            requirements: 요구사항
            model_name: 사용할 모델 이름
            preferred_language: 선호 언어
            max_retry_count: 최대 재시도 횟수
            
        Returns:
            BoundedContextStructureModel
        """
        last_exception = None
        
        for retry_count in range(max_retry_count):
            try:
                # CreateDraftGenerator 초기화
                client = {
                    "inputs": {
                        "bounded_context_info": bounded_context_info.model_dump(),
                        "requirements": requirements
                    },
                    "preferredLanguage": preferred_language,
                    "retryCount": retry_count
                }
                
                generator = CreateDraftGenerator(
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
                output: CreateDraftGeneratorOutput = result.get("result")
                
                # 데이터 정합성 검증 및 중복 제거
                validated_output = CreateDraftGeneratorUtil._validate_and_deduplicate(output)
                
                # BoundedContextStructureModel로 변환
                structure_model = CreateDraftGeneratorUtil._convert_to_structure_model(
                    validated_output,
                    bounded_context_info
                )
                
                # 최종 검증
                if CreateDraftGeneratorUtil._is_valid_structure(structure_model):
                    return structure_model
                else:
                    raise ValueError("Generated structure failed final validation")
                    
            except Exception as e:
                last_exception = e
                if retry_count == max_retry_count - 1:
                    raise RuntimeError(
                        f"Failed to generate draft after {max_retry_count} attempts. Last error: {str(last_exception)}"
                    ) from last_exception
        
        # This should never be reached, but just in case
        raise RuntimeError(f"Unexpected error in draft generation") from last_exception
    
    @staticmethod
    def _validate_and_deduplicate(output: CreateDraftGeneratorOutput) -> CreateDraftGeneratorOutput:
        """
        생성된 출력의 중복을 제거하고 유효성을 검증합니다.
        
        Args:
            output: CreateDraftGeneratorOutput
            
        Returns:
            검증 및 중복 제거된 CreateDraftGeneratorOutput
        """
        if not output or not output.aggregates:
            raise ValueError("Output has no aggregates")
        
        # Aggregate 중복 제거 (name 기준)
        seen_aggregate_names: Set[str] = set()
        unique_aggregates = []
        
        for aggregate in output.aggregates:
            # 빈 이름 체크
            if not aggregate.aggregateName or not aggregate.aggregateAlias:
                continue
            
            # 이름 정규화 (공백 제거 및 대소문자 처리)
            normalized_name = aggregate.aggregateName.strip()
            
            if normalized_name.lower() in seen_aggregate_names:
                continue
            
            # Enumeration 중복 제거
            aggregate.enumerations = CreateDraftGeneratorUtil._deduplicate_enumerations(
                aggregate.enumerations
            )
            
            # ValueObject 중복 제거
            aggregate.valueObjects = CreateDraftGeneratorUtil._deduplicate_value_objects(
                aggregate.valueObjects
            )
            
            seen_aggregate_names.add(normalized_name.lower())
            unique_aggregates.append(aggregate)
        
        if not unique_aggregates:
            raise ValueError("No valid aggregates after deduplication")
        
        output.aggregates = unique_aggregates
        return output
    
    @staticmethod
    def _deduplicate_enumerations(enumerations: List[EnumerationInfoModel]) -> List[EnumerationInfoModel]:
        """Enumeration 리스트에서 중복을 제거합니다."""
        if not enumerations:
            return []
        
        seen_names: Set[str] = set()
        unique_enumerations = []
        
        for enum in enumerations:
            if not enum.name or not enum.alias:
                continue
            
            normalized_name = enum.name.strip()
            if normalized_name.lower() in seen_names:
                continue
            
            seen_names.add(normalized_name.lower())
            unique_enumerations.append(enum)
        
        return unique_enumerations
    
    @staticmethod
    def _deduplicate_value_objects(value_objects: List[ValueObjectInfoNoRefModel]) -> List[ValueObjectInfoNoRefModel]:
        """ValueObject 리스트에서 중복을 제거합니다."""
        if not value_objects:
            return []
        
        seen_names: Set[str] = set()
        unique_value_objects = []
        
        for vo in value_objects:
            if not vo.name or not vo.alias:
                continue
            
            normalized_name = vo.name.strip()
            if normalized_name.lower() in seen_names:
                continue
            
            seen_names.add(normalized_name.lower())
            unique_value_objects.append(vo)
        
        return unique_value_objects
    
    @staticmethod
    def _convert_to_structure_model(
        output: CreateDraftGeneratorOutput,
        bounded_context_info: BoundedContextInfoModel
    ) -> BoundedContextStructureModel:
        """
        CreateDraftGeneratorOutput을 BoundedContextStructureModel로 변환합니다.
        
        Args:
            output: CreateDraftGeneratorOutput
            bounded_context_info: BoundedContextInfoModel
            
        Returns:
            BoundedContextStructureModel
        """
        aggregates = []
        
        for agg_info in output.aggregates:
            # Enumeration 변환
            enumerations = [
                EnumerationInfoModel(
                    name=enum.name,
                    alias=enum.alias
                )
                for enum in agg_info.enumerations
            ]
            
            # ValueObject 변환
            value_objects = [
                ValueObjectInfoModel(
                    name=vo.name,
                    alias=vo.alias,
                    referencedAggregate=None
                )
                for vo in agg_info.valueObjects
            ]
            
            # AggregateInfoModel 생성
            aggregate = AggregateInfoModel(
                aggregateName=agg_info.aggregateName,
                aggregateAlias=agg_info.aggregateAlias,
                enumerations=enumerations,
                valueObjects=value_objects
            )
            
            aggregates.append(aggregate)
        
        # BoundedContextStructureModel 생성
        structure_model = BoundedContextStructureModel(
            boundedContextName=bounded_context_info.name,
            boundedContextAlias=bounded_context_info.alias,
            aggregates=aggregates
        )
        
        return structure_model
    
    @staticmethod
    def _is_valid_structure(structure_model: BoundedContextStructureModel) -> bool:
        """
        생성된 BoundedContextStructureModel의 유효성을 검증합니다.
        
        Args:
            structure_model: 검증할 BoundedContextStructureModel
            
        Returns:
            유효성 여부
        """
        # 기본 필드 검증
        if not structure_model.boundedContextName or not structure_model.boundedContextAlias:
            return False
        
        # Aggregate 존재 검증
        if not structure_model.aggregates or len(structure_model.aggregates) == 0:
            return False
        
        # 각 Aggregate 검증
        for aggregate in structure_model.aggregates:
            if not aggregate.aggregateName or not aggregate.aggregateAlias:
                return False
        
        return True