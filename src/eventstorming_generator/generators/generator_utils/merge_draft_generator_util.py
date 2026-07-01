from typing import Dict, List, Any

from ...models import BoundedContextStructureModel, AggregateInfoModel, EnumerationInfoModel, ValueObjectInfoModel, ReferencedAggregateInfoModel, MergeDraftGeneratorOutput, MergedDraftInfo, MergedAggregateInfo
from ..merge_draft_generator import MergeDraftGenerator

class MergeDraftGeneratorUtil:
    @staticmethod
    def sequential_merge_drafts_safely(
        target_drafts: List[BoundedContextStructureModel], 
        model_name: str,
        preferred_language: str,
        accumulate_count: int, 
        max_retry_count: int,
        job_id: str
    ) -> List[BoundedContextStructureModel]:
        """
        타겟 드래프트를 배치로 나누어 순차적으로 병합합니다.
        각 배치마다 재시도 로직과 검증 로직, 필터링 로직이 포함되어서 안전하게 병합합니다.
        
        Args:
            target_drafts: 병합할 구조 모델 리스트
            model_name: LLM 모델 이름
            preferred_language: 선호하는 응답 언어
            accumulate_count: 한 배치에 포함할 최대 BC 개수
            max_retry_count: 각 배치당 최대 재시도 횟수
            
        Returns:
            병합된 구조 모델 리스트
        """
        if not target_drafts:
            return []
        
        # List를 Dict로 변환 (BC 이름을 키로)
        target_drafts_dict = {structure.boundedContextName: structure for structure in target_drafts}
        
        # BC 이름 목록 추출
        bc_names = list(target_drafts_dict.keys())
        accumulated_drafts: Dict[str, BoundedContextStructureModel] = {}
        
        # 배치 생성
        batches = []
        for i in range(0, len(bc_names), accumulate_count):
            batches.append(bc_names[i:i + accumulate_count])
        
        # 각 배치 처리
        for batch_idx, batch_bc_names in enumerate(batches):
            batch_retry_count = 0
            remaining_bc_names = batch_bc_names.copy()
            
            while remaining_bc_names and batch_retry_count < max_retry_count:
                try:
                    # accumulatedDrafts에 없는 모든 BC를 targetDrafts에 포함
                    target_bc_names_for_generator = [
                        name for name in target_drafts_dict.keys() 
                        if name not in accumulated_drafts
                    ]
                    
                    # Generator 입력 형식으로 변환
                    generator_target_drafts = MergeDraftGeneratorUtil._structures_to_generator_dict(
                        {name: target_drafts_dict[name] for name in target_bc_names_for_generator}
                    )
                    generator_accumulated_drafts = MergeDraftGeneratorUtil._structures_to_generator_dict(
                        accumulated_drafts
                    )
                    
                    # Generator 호출
                    generator = MergeDraftGenerator(
                        model_name=model_name,
                        client={
                            "inputs": {
                                "targetDrafts": generator_target_drafts,
                                "accumulatedDrafts": generator_accumulated_drafts,
                                "targetBoundedContextNames": remaining_bc_names
                            },
                            "preferredLanguage": preferred_language,
                            "retryCount": batch_retry_count
                        }
                    )
                    
                    result = generator.generate(
                        bypass_cache=(batch_retry_count > 0),
                        retry_count=batch_retry_count,
                        extra_config_metadata={
                            "job_id": job_id
                        }
                    )
                    output: MergeDraftGeneratorOutput = result["result"]
                    
                    # 중복 Aggregate 제거 (예외 처리)
                    output = MergeDraftGeneratorUtil._remove_duplicate_aggregates(output)
                    
                    # 생성된 BC 확인
                    generated_bc_names = [draft.boundedContextName for draft in output.mergedDrafts]
                    
                    # 생성된 결과를 BoundedContextStructureModel로 변환하여 누적
                    batch_result = MergeDraftGeneratorUtil._generator_output_to_structures(output)
                    accumulated_drafts.update(batch_result)
                    
                    # 누락된 BC 확인
                    missing_bc_names = [name for name in remaining_bc_names if name not in generated_bc_names]
                    
                    if missing_bc_names:
                        remaining_bc_names = missing_bc_names
                        batch_retry_count += 1
                    else:
                        remaining_bc_names = []
                        break
                        
                except Exception as e:
                    batch_retry_count += 1
                    
                    if batch_retry_count == 0:
                        raise RuntimeError(
                            f"배치 {batch_idx + 1} 처리 실패. 누락된 BC: {remaining_bc_names}"
                        ) from e
            
            # 재시도 소진 후에도 누락된 BC가 있는 경우
            if remaining_bc_names:
                raise RuntimeError(
                    f"배치 {batch_idx + 1} 재시도 소진. 처리되지 않은 BC: {remaining_bc_names}"
                )
        
        # Dict를 List로 변환
        result_structures = list(accumulated_drafts.values())
        
        # 유효하지 않은 IDValueObjects 필터링
        result_structures = MergeDraftGeneratorUtil._filter_invalid_id_value_objects(result_structures)
        
        return result_structures
    
    @staticmethod
    def _remove_duplicate_aggregates(output: MergeDraftGeneratorOutput) -> MergeDraftGeneratorOutput:
        """
        동일한 AggregateName을 가진 중복 MergedAggregateInfo를 제거합니다.
        각 BoundedContext 내에서 aggregateName이 중복되는 경우, 
        가장 많은 정보를 가진 하나의 Aggregate만 유지합니다.
        
        Args:
            output: MergeDraftGeneratorOutput 인스턴스
            
        Returns:
            중복이 제거된 MergeDraftGeneratorOutput 인스턴스
        """
        
        cleaned_drafts = []
        
        for draft in output.mergedDrafts:
            # aggregateName을 키로 하는 딕셔너리 생성
            aggregate_dict: Dict[str, MergedAggregateInfo] = {}
            
            for aggregate in draft.aggregates:
                agg_name = aggregate.aggregateName
                
                if agg_name not in aggregate_dict:
                    # 첫 번째 발견
                    aggregate_dict[agg_name] = aggregate
                else:
                    # 중복 발견 - 더 많은 정보를 가진 것을 선택
                    existing_count = (
                        len(aggregate_dict[agg_name].valueObjects) +
                        len(aggregate_dict[agg_name].enumerations) +
                        len(aggregate_dict[agg_name].IDValueObjects)
                    )
                    new_count = (
                        len(aggregate.valueObjects) +
                        len(aggregate.enumerations) +
                        len(aggregate.IDValueObjects)
                    )
                    
                    if new_count > existing_count:
                        aggregate_dict[agg_name] = aggregate
            
            # 중복이 제거된 aggregates 리스트 생성
            unique_aggregates = list(aggregate_dict.values())
            
            # 새로운 MergedDraftInfo 생성
            cleaned_draft = MergedDraftInfo(
                boundedContextName=draft.boundedContextName,
                aggregates=unique_aggregates
            )
            cleaned_drafts.append(cleaned_draft)
        
        # 새로운 Output 생성
        return MergeDraftGeneratorOutput(mergedDrafts=cleaned_drafts)
    
    @staticmethod
    def _structures_to_generator_dict(structures_dict: Dict[str, BoundedContextStructureModel]) -> Dict[str, Any]:
        """
        Dict[str, BoundedContextStructureModel]을 Generator 입력 형식으로 변환
        
        Args:
            structures_dict: BC 이름을 키로 가지는 BoundedContextStructureModel 딕셔너리
            
        Returns:
            Generator 입력용 딕셔너리
        """
        result = {}
        
        for bc_name, structure in structures_dict.items():
            
            aggregates_list = []
            for agg in structure.aggregates:
                aggregate_dict = {
                    "aggregateName": agg.aggregateName,
                    "aggregateAlias": agg.aggregateAlias,
                    "enumerations": [
                        {
                            "name": enum.name,
                            "alias": enum.alias
                        }
                        for enum in agg.enumerations
                    ],
                    "valueObjects": []
                }
                
                # ValueObjects 처리 (IDValueObjects와 일반 ValueObjects 분리)
                id_value_objects = []
                regular_value_objects = []
                
                for vo in agg.valueObjects:
                    if vo.referencedAggregate:
                        # IDValueObject
                        id_value_objects.append({
                            "name": vo.name,
                            "alias": vo.alias,
                            "referencedAggregateName": vo.referencedAggregate.name,
                            "referencedAggregateAlias": vo.referencedAggregate.alias
                        })
                    else:
                        # 일반 ValueObject
                        regular_value_objects.append({
                            "name": vo.name,
                            "alias": vo.alias
                        })
                
                aggregate_dict["valueObjects"] = regular_value_objects
                aggregate_dict["IDValueObjects"] = id_value_objects
                
                aggregates_list.append(aggregate_dict)
            
            result[bc_name] = {
                "aggregates": aggregates_list
            }
        
        return result
    
    @staticmethod
    def _generator_output_to_structures(output: MergeDraftGeneratorOutput) -> Dict[str, BoundedContextStructureModel]:
        """
        Generator 출력을 Dict[str, BoundedContextStructureModel]로 변환
        
        Args:
            output: MergeDraftGeneratorOutput 인스턴스
            
        Returns:
            BC 이름을 키로 가지는 BoundedContextStructureModel 딕셔너리
        """
        result = {}
        
        for merged_draft in output.mergedDrafts:
            bc_name = merged_draft.boundedContextName
            
            aggregates = []
            for merged_agg in merged_draft.aggregates:
                # Enumerations 변환
                enumerations = [
                    EnumerationInfoModel(
                        name=enum.name,
                        alias=enum.alias
                    )
                    for enum in merged_agg.enumerations
                ]
                
                # ValueObjects 변환 (일반 + ID ValueObjects)
                value_objects = []
                
                # 일반 ValueObjects
                for vo in merged_agg.valueObjects:
                    value_objects.append(
                        ValueObjectInfoModel(
                            name=vo.name,
                            alias=vo.alias,
                            referencedAggregate=None
                        )
                    )
                
                # ID ValueObjects
                for id_vo in merged_agg.IDValueObjects:
                    value_objects.append(
                        ValueObjectInfoModel(
                            name=id_vo.name,
                            alias=id_vo.alias,
                            referencedAggregate=ReferencedAggregateInfoModel(
                                name=id_vo.referencedAggregateName,
                                alias=id_vo.referencedAggregateAlias
                            )
                        )
                    )
                
                # Aggregate 생성
                aggregates.append(
                    AggregateInfoModel(
                        aggregateName=merged_agg.aggregateName,
                        aggregateAlias=merged_agg.aggregateAlias,
                        enumerations=enumerations,
                        valueObjects=value_objects
                    )
                )
            
            # BoundedContextStructureModel 생성
            structure = BoundedContextStructureModel(
                boundedContextName=bc_name,
                boundedContextAlias=bc_name,  # alias는 동일하게 설정
                aggregates=aggregates
            )
            
            result[bc_name] = structure
        
        return result
    
    @staticmethod
    def _filter_invalid_id_value_objects(structures: List[BoundedContextStructureModel]) -> List[BoundedContextStructureModel]:
        """
        존재하지 않는 Aggregate를 참조하는 IDValueObjects를 제거합니다.
        
        Args:
            structures: BoundedContextStructureModel 리스트
            
        Returns:
            유효하지 않은 IDValueObjects가 제거된 BoundedContextStructureModel 리스트
        """
        # 1. 모든 존재하는 Aggregate 이름 수집
        valid_aggregate_names = set()
        for structure in structures:
            for aggregate in structure.aggregates:
                valid_aggregate_names.add(aggregate.aggregateName)
        
        # 2. 각 Aggregate의 ValueObjects 필터링
        filtered_structures = []
        for structure in structures:
            filtered_aggregates = []
            
            for aggregate in structure.aggregates:
                # ValueObjects 필터링 (referencedAggregate가 유효한 것만 유지)
                filtered_value_objects = []
                for vo in aggregate.valueObjects:
                    # referencedAggregate가 없거나 (일반 ValueObject)
                    # referencedAggregate.name이 유효한 Aggregate를 가리키는 경우만 유지
                    if vo.referencedAggregate is None or vo.referencedAggregate.name in valid_aggregate_names:
                        filtered_value_objects.append(vo)
                
                # 필터링된 ValueObjects로 새로운 Aggregate 생성
                filtered_aggregate = AggregateInfoModel(
                    aggregateName=aggregate.aggregateName,
                    aggregateAlias=aggregate.aggregateAlias,
                    enumerations=aggregate.enumerations,
                    valueObjects=filtered_value_objects
                )
                filtered_aggregates.append(filtered_aggregate)
            
            # 필터링된 Aggregates로 새로운 Structure 생성
            filtered_structure = BoundedContextStructureModel(
                boundedContextName=structure.boundedContextName,
                boundedContextAlias=structure.boundedContextAlias,
                aggregates=filtered_aggregates
            )
            filtered_structures.append(filtered_structure)
        
        return filtered_structures