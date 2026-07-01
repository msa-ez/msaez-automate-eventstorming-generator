import json
import logging
from typing import Any


logger = logging.getLogger(__name__)


class JobRequirementsUtil:
    @staticmethod
    def parse_requirements(requirements: str) -> str:
        """
        requirements 문자열에서 JSON 구조를 찾아 Markdown으로 변환합니다.
        
        Args:
            requirements: JSON 객체를 포함할 수 있는 문자열
            
        Returns:
            JSON이 Markdown으로 변환된 문자열. 변환 실패 시 원본 반환.
        """
        try:
            def find_json_blocks(text: str) -> list[tuple[int, int, str]]:
                """중첩된 JSON 블록을 찾습니다."""
                blocks = []
                i = 0
                while i < len(text):
                    if text[i] == '{':
                        depth = 1
                        start = i
                        i += 1
                        while i < len(text) and depth > 0:
                            if text[i] == '{':
                                depth += 1
                            elif text[i] == '}':
                                depth -= 1
                            i += 1
                        if depth == 0:
                            blocks.append((start, i, text[start:i]))
                    else:
                        i += 1
                return blocks
            
            json_blocks = find_json_blocks(requirements)
            
            if not json_blocks:
                return requirements
            
            result = requirements
            offset = 0
            
            for start, end, json_str in json_blocks:
                try:
                    json_obj = json.loads(json_str)
                    markdown = JobRequirementsUtil._json_to_markdown(json_obj, level=1)
                    
                    adjusted_start = start + offset
                    adjusted_end = end + offset
                    result = result[:adjusted_start] + markdown + result[adjusted_end:]
                    offset += len(markdown) - (end - start)
                except json.JSONDecodeError:
                    continue
            
            return result
            
        except Exception as e:
            logger.exception(f"Failed to parse requirements: {e}")
            return requirements
    
    @staticmethod
    def _json_to_markdown(obj: Any, level: int = 1) -> str:
        """
        JSON 객체를 Markdown으로 변환합니다.
        
        Args:
            obj: 변환할 JSON 객체 (dict, list, 또는 기본 타입)
            level: 현재 헤더 레벨 (1부터 시작)
            
        Returns:
            Markdown 형식의 문자열
        """
        lines = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                header = '#' * level
                lines.append(f"{header} {key}")
                
                if isinstance(value, dict):
                    lines.append(JobRequirementsUtil._json_to_markdown(value, level + 1))
                elif isinstance(value, list):
                    lines.append(JobRequirementsUtil._format_list(value, level + 1))
                else:
                    lines.append(JobRequirementsUtil._format_value(value))
        elif isinstance(obj, list):
            lines.append(JobRequirementsUtil._format_list(obj, level))
        else:
            lines.append(JobRequirementsUtil._format_value(obj))
        
        return '\n'.join(lines)
    
    @staticmethod
    def _format_list(lst: list, level: int) -> str:
        """
        리스트를 Markdown bullet list로 변환합니다.
        
        Args:
            lst: 변환할 리스트
            level: 현재 헤더 레벨
            
        Returns:
            Markdown bullet list 형식의 문자열
        """
        lines = []
        for item in lst:
            if isinstance(item, dict):
                dict_md = JobRequirementsUtil._json_to_markdown(item, level)
                lines.append(dict_md)
            elif isinstance(item, list):
                nested_list = JobRequirementsUtil._format_list(item, level)
                lines.append(nested_list)
            else:
                formatted_value = JobRequirementsUtil._format_value(item)
                lines.append(f"- {formatted_value}")
        return '\n'.join(lines)
    
    @staticmethod
    def _format_value(value: Any) -> str:
        """
        기본 값을 문자열로 포맷합니다.
        
        Args:
            value: 포맷할 값
            
        Returns:
            포맷된 문자열
        """
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, str):
            return value
        return str(value)
