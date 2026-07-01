from typing import Dict, List
from ...models import ReferencedContextMapping
from ...types import LineNumberRange

class CreateContextMappingUtil:
    @staticmethod
    def get_referenced_context_mappings(line_number_ranges: Dict[str, List[LineNumberRange]], requirements: str) -> List[ReferencedContextMapping]:
        """
        바운디드 컨텍스트별로 참조된 요구사항을 추출하고 매핑 정보를 생성합니다.
        
        Args:
            line_number_ranges: 각 바운디드 컨텍스트별 참조한 라인 범위 정보
            requirements: 원본 요구사항 문자열
            
        Returns:
            ReferencedContextMapping 리스트
        """
        if not line_number_ranges or len(line_number_ranges) == 0:
            raise RuntimeError("Line number ranges is empty")
        
        if not requirements or len(requirements) == 0:
            raise RuntimeError("Requirements is empty")


        result = []
        
        # 요구사항을 라인 단위로 분리
        requirement_lines = requirements.split('\n')
        
        for bounded_context_name, ranges in line_number_ranges.items():
            # 라인 범위를 유효 범위로 조정 후 병합
            clipped_ranges = CreateContextMappingUtil._clip_line_ranges(ranges, len(requirement_lines))
            merged_ranges = CreateContextMappingUtil._merge_line_ranges(clipped_ranges)
            
            # 요구사항 추출 및 매핑 생성
            created_requirements_lines = []
            requirement_index_mapping = {}
            new_line_number = 1
            
            for start_line, end_line in merged_ranges:
                # 1-based index를 0-based index로 변환
                for original_line_number in range(start_line, end_line + 1):
                    array_index = original_line_number - 1
                    
                    # 범위 체크
                    if 0 <= array_index < len(requirement_lines):
                        created_requirements_lines.append(requirement_lines[array_index])
                        requirement_index_mapping[new_line_number] = original_line_number
                        new_line_number += 1
            
            # 추출된 라인들을 개행으로 조합
            created_requirements = '\n'.join(created_requirements_lines)
            
            # ReferencedContextMapping 객체 생성
            referenced_context_mapping = ReferencedContextMapping(
                bounded_context_name=bounded_context_name,
                requirement_index_mapping=requirement_index_mapping,
                created_requirements=created_requirements
            )
            
            result.append(referenced_context_mapping)
        
        return result
    
    @staticmethod
    def _merge_line_ranges(ranges: List[LineNumberRange]) -> List[LineNumberRange]:
        """
        겹치거나 인접한 라인 범위들을 병합합니다.
        
        Args:
            ranges: 라인 범위 리스트 [[start, end], ...]
            
        Returns:
            병합된 라인 범위 리스트
        """
        if not ranges:
            return []
        
        # 시작 지점 기준으로 정렬
        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        
        merged = [sorted_ranges[0]]
        
        for current_range in sorted_ranges[1:]:
            last_merged = merged[-1]
            
            # 현재 범위가 마지막 병합된 범위와 겹치거나 인접한 경우
            if current_range[0] <= last_merged[1] + 1:
                # 병합: 끝 지점을 더 큰 값으로 업데이트
                merged[-1] = [last_merged[0], max(last_merged[1], current_range[1])]
            else:
                # 겹치지 않으면 새로운 범위로 추가
                merged.append(current_range)
        
        return merged
    
    @staticmethod
    def _clip_line_ranges(ranges: List[LineNumberRange], max_line: int) -> List[LineNumberRange]:
        """
        라인 범위를 유효 범위(1 ~ max_line)로 조정(clipping)합니다.
        
        Args:
            ranges: 라인 범위 리스트 [[start, end], ...]
            max_line: 요구사항의 최대 라인 번호
            
        Returns:
            유효 범위로 조정된 라인 범위 리스트
        """
        if not ranges:
            return []
        
        if max_line < 1:
            return []
        
        min_line = 1
        clipped = []
        
        for range_item in ranges:
            start, end = range_item[0], range_item[1]
            
            # 유효 범위로 clipping
            clipped_start = max(min_line, min(start, max_line))
            clipped_end = max(min_line, min(end, max_line))
            
            clipped.append([clipped_start, clipped_end])
        
        return clipped