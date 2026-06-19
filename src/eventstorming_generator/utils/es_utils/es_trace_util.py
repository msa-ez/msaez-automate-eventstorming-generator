from typing import Any, List, Tuple, Optional
import re

from ..log_util import LogUtil
from ...models import ActionModel
from ...types import RequirementIndexMapping
from ...models import State

class EsTraceUtil:
    @staticmethod
    def add_line_numbers_to_description(description: str, base_line: int = 1) -> str:
        """
        기능 요구사항 텍스트에 줄 번호를 추가하는 공통 메서드
        
        Args:
            description: 원본 기능 요구사항 텍스트

        Returns:
            줄 번호가 추가된 텍스트
        """
        if not description:
            return description
            
        lines = description.split('\n')
        line_numbered_description = '\n'.join([f"<{i+base_line}>{line}</{i+base_line}>" for i, line in enumerate(lines)])
        return line_numbered_description

    @staticmethod
    def get_line_number_range(line_numbered_requirements: str) -> Tuple[int, int]:
        """
        줄 번호가 붙은 텍스트에서 최소/최대 줄 번호를 추출합니다.
        
        Args:
            line_numbered_requirements: "<1>내용</1>\n<2>내용</2>..." 형태의 텍스트
            
        Returns:
            (min_line, max_line) 튜플
        """
        if not line_numbered_requirements:
            return (1, 1)
        
        line_numbers = []
        for line in line_numbered_requirements.split('\n'):
            match = re.match(r'^<(\d+)>.*</\1>$', line)
            if match:
                line_numbers.append(int(match.group(1)))
        
        if not line_numbers:
            return (1, 1)
        
        return (min(line_numbers), max(line_numbers))

    @staticmethod
    def convert_refs_to_indexes(actions: List[ActionModel], original_description: str, requirement_index_mapping: Optional[RequirementIndexMapping], state: State, log_prefix: str = "", full_requirements_text: Optional[str] = None) -> None:
        """
        Action 목록의 모든 refs를 단어 조합에서 컬럼 위치로 변환하는 공통 메서드
        JavaScript의 sanitizeAndConvertRefs와 동일한 3단계 처리 방식 적용

        Args:
            actions: 변환할 액션 목록
            original_description: 원본 기능 요구사항 텍스트 (BC-chunked, 줄 번호 없는 버전)
            requirement_index_mapping: BC-local 라인 번호 → 원본 userStory 라인 번호 매핑
            state: 로깅을 위한 State 객체
            log_prefix: 로그 메시지에 사용할 접두사
            full_requirements_text: 전체 userStory 텍스트 (선택).
                있으면 노이즈 필터가 mapping 이후의 원본 userStory 라인 내용으로 구조 라인 판정.
                없으면 BC-chunked text 기준으로 판정 (mapping 이전 좌표).
        """
        if not original_description:
            LogUtil.add_warning_log(state, f"{log_prefix} Original description is empty, skipping source reference conversion.")
            return

        # 1. 준비 작업: line numbering 및 범위 계산
        line_numbered_description = EsTraceUtil.add_line_numbers_to_description(original_description)
        min_line, max_line = EsTraceUtil.get_line_number_range(line_numbered_description)
        lines = original_description.split('\n')  # prefix 없는 원본 라인들 (BC-chunked)

        # 노이즈 필터용 라인: 가능하면 full userStory 사용 (mapping 적용 후 좌표와 일치).
        # 없으면 BC-chunked lines 로 fallback (mapping 이전 좌표 기준 — 부정확하지만 차선).
        filter_lines = full_requirements_text.split('\n') if full_requirements_text else lines

        for action in actions:
            args = action.args
            if not args:
                continue

            # Aggregate, ValueObject, Enumeration, Command, Event, ReadModel 자체의 refs 변환
            if args.get("refs"):
                refs_to_process = args["refs"]
                if EsTraceUtil._needs_processing(refs_to_process):
                    try:
                        # 3단계 처리 (sanitize → transform → clamp → 노이즈필터 → mapping)
                        processed_refs = EsTraceUtil._process_refs_with_three_stages(
                            refs_to_process, lines, min_line, max_line,
                            requirement_index_mapping, state, log_prefix
                        )
                        args["refs"] = processed_refs
                    except Exception as e:
                        LogUtil.add_warning_log(state, f"{log_prefix} Failed to process refs for action: {e}")
                else:
                    # numeric refs 경로 — 3단계 skip 됐으니 BC-local → 원본 좌표 mapping 을 여기서 적용해야
                    # filter_lines (full userStory) 기준 노이즈 필터가 정확하게 동작함.
                    if requirement_index_mapping:
                        try:
                            args["refs"] = EsTraceUtil._apply_requirement_index_mapping(
                                args["refs"], requirement_index_mapping
                            )
                        except Exception as e:
                            LogUtil.add_warning_log(state, f"{log_prefix} Failed to apply mapping for action: {e}")
                # 노이즈 필터 — full userStory 좌표 기준으로 zero-length / 구조 라인 start drop.
                try:
                    args["refs"] = EsTraceUtil._filter_structural_and_zero_length(
                        args["refs"], filter_lines, state, log_prefix
                    )
                except Exception as e:
                    LogUtil.add_warning_log(state, f"{log_prefix} Failed to filter refs for action: {e}")

            # Properties 또는 queryParameters의 refs 변환
            properties_key = None
            if args.get("properties"):
                properties_key = "properties"
            elif args.get("queryParameters"):
                properties_key = "queryParameters"

            if properties_key and args.get(properties_key):
                for prop in args[properties_key]:
                    if isinstance(prop, dict) and prop.get("refs"):
                        refs_to_process = prop["refs"]
                        if EsTraceUtil._needs_processing(refs_to_process):
                            try:
                                # 3단계 처리
                                processed_refs = EsTraceUtil._process_refs_with_three_stages(
                                    refs_to_process, lines, min_line, max_line,
                                    requirement_index_mapping, state, log_prefix
                                )
                                prop["refs"] = processed_refs
                            except Exception as e:
                                LogUtil.add_warning_log(state, f"{log_prefix} Failed to process refs for property: {e}")
                        else:
                            # numeric refs 경로 — mapping 직접 적용
                            if requirement_index_mapping:
                                try:
                                    prop["refs"] = EsTraceUtil._apply_requirement_index_mapping(
                                        prop["refs"], requirement_index_mapping
                                    )
                                except Exception as e:
                                    LogUtil.add_warning_log(state, f"{log_prefix} Failed to apply mapping for property: {e}")
                        # 노이즈 필터 — full userStory 좌표 기준
                        try:
                            prop["refs"] = EsTraceUtil._filter_structural_and_zero_length(
                                prop["refs"], filter_lines, state, log_prefix
                            )
                        except Exception as e:
                            LogUtil.add_warning_log(state, f"{log_prefix} Failed to filter refs for property: {e}")

    @staticmethod
    def _needs_processing(refs_array: List[List[List[Any]]]) -> bool:
        """refs가 처리가 필요한지 확인 (문자열 기반 참조가 있는지)"""
        if not refs_array:
            return False
            
        for reference in refs_array:
            if not isinstance(reference, list):
                continue
            for position_pair in reference:
                if not isinstance(position_pair, list) or len(position_pair) != 2:
                    continue
                # 두 번째 요소가 문자열이면 처리 필요
                if isinstance(position_pair[1], str):
                    return True
        return False

    @staticmethod  
    def _process_refs_with_three_stages(refs_array: List[List[List[Any]]], lines: List[str], min_line: int, max_line: int, requirement_index_mapping: Optional[RequirementIndexMapping], state: State, log_prefix: str) -> List[List[List[int]]]:
        """
        3단계 처리를 통해 refs를 안정적으로 변환합니다.
        
        Args:
            refs_array: 처리할 refs 배열
            lines: 원본 텍스트의 줄 목록 (줄 번호 prefix 없는 상태)
            min_line: 유효한 최소 줄 번호
            max_line: 유효한 최대 줄 번호
            state: 로깅을 위한 State 객체
            log_prefix: 로그 메시지에 사용할 접두사
            
        Returns:
            처리된 refs 배열
        """
        try:
            # 1단계: refs 배열 정리 (구문/라인 검증)
            sanitized_refs = EsTraceUtil._sanitize_refs_array(refs_array, lines, min_line, max_line)

            # 2단계: 칼럼 좌표 변환 (문자열 -> 인덱스)
            converted_refs = EsTraceUtil._transform_reference(sanitized_refs, lines, state, log_prefix)

            # 3단계: 최종 클램핑 처리
            final_refs = EsTraceUtil._clamp_refs_array(converted_refs, lines, min_line, max_line)

            # 3.5단계: 노이즈 필터 — zero-length / markdown 구조 라인(헤더/표/구분선/공백) start drop.
            # LLM 이 가이드를 안 지킬 때 산출물 추적성에서 헤더·표 row 가 highlight 시작점이 되는 케이스 방지.
            # (project_generator 의 requirements_mapper / preview_fields_generator / traceability_generator 와 동일 정책)
            final_refs = EsTraceUtil._filter_structural_and_zero_length(final_refs, lines, state, log_prefix)

            # 4단계: requirement_index_mapping이 있을 경우, 인덱스 맵핑 적용
            if requirement_index_mapping:
                final_refs = EsTraceUtil._apply_requirement_index_mapping(final_refs, requirement_index_mapping)

            return final_refs
            
        except Exception as e:
            LogUtil.add_warning_log(state, f"{log_prefix} Error in 3-stage processing: {e}")
            # 실패 시 기존 로직으로 fallback
            return EsTraceUtil._transform_reference(refs_array, lines, state, log_prefix)

    @staticmethod
    def _transform_reference(reference: List[List[List[Any]]], lines: List[str], state: State, log_prefix: str) -> List[List[List[int]]]:
        """
        단일 refs 값을 변환합니다.
        
        Args:
            reference: 변환할 refs 값
            lines: 원본 텍스트의 줄 목록
            state: 로깅을 위한 State 객체
            log_prefix: 로그 메시지에 사용할 접두사
            
        Returns:
            변환된 refs 값
        """
        transformed_positions = []
        for position_pair in reference:
            try:
                if not position_pair:
                    continue

                start_pos = position_pair[0]
                end_pos = position_pair[1] if len(position_pair) > 1 else None

                start_line_num, start_words = start_pos
                start_line_num = int(start_line_num)

                if isinstance(start_words, int):
                    start_col = start_words
                else:
                    start_col = EsTraceUtil._find_column_for_words(lines, start_line_num, start_words)

                if end_pos:
                    end_line_num, end_words = end_pos
                    end_line_num = int(end_line_num)
                    
                    if isinstance(end_words, int):
                        end_col = end_words
                    else:
                        end_col = EsTraceUtil._find_column_for_words(lines, end_line_num, end_words, is_end=True)
                else:
                    end_line_num = start_line_num
                    end_words = ""
                    if 1 <= start_line_num <= len(lines):
                        line_content = lines[start_line_num - 1]
                        end_col = len(line_content)
                    else:
                        end_col = -1

                # 컬럼 값 유효성 추가 검증
                if start_col > 0 and end_col > 0 and start_col <= end_col:
                    transformed_positions.append([[start_line_num, start_col], [end_line_num, end_col]])
                elif start_col > 0 and end_col > 0:
                    # 순서가 바뀐 경우 보정
                    transformed_positions.append([[start_line_num, min(start_col, end_col)], [end_line_num, max(start_col, end_col)]])
                else:
                    log_end_line = end_line_num if end_pos else start_line_num
                    log_end_words = f"'{end_words}'" if end_pos else "<end of line>"
                    LogUtil.add_warning_log(state, f"{log_prefix} Could not find words for reference. Start: L{start_line_num} '{start_words}' (col: {start_col}), End: L{log_end_line} {log_end_words} (col: {end_col}). Skipping this reference.")

            except (ValueError, IndexError, TypeError) as e:
                LogUtil.add_warning_log(state, f"{log_prefix} Malformed refs item '{position_pair}'. Error: {e}. Skipping.")
                continue
        return transformed_positions

    @staticmethod
    def _find_column_for_words(lines: List[str], line_num: int, words: str, is_end: bool = False) -> int:
        """
        주어진 라인 번호와 단어 조합으로 컬럼 위치를 찾습니다.
        JavaScript의 _findColumnForWords와 동일한 로직 (partial matching 포함)
        
        Args:
            lines: 텍스트의 줄 목록
            line_num: 라인 번호 (1-based)
            words: 찾을 단어 조합
            is_end: True이면 단어 조합의 끝 위치를 반환
            
        Returns:
            컬럼 위치 (1-based), 찾지 못하면 적절한 기본값
        """
        if not (1 <= line_num <= len(lines)):
            return 1
        
        line_content = lines[line_num - 1]
        
        # 라인 내용이 없는 경우
        if not line_content:
            return 1
            
        # 단어가 없는 경우
        if not words or not isinstance(words, str):
            return len(line_content) if is_end else 1
        
        # 1차: 정확한 문구 찾기
        try:
            index = line_content.index(words)
            return index + len(words) if is_end else index + 1
        except ValueError:
            pass
        
        # 2차: 매칭 실패 시 기본값 반환
        return len(line_content) if is_end else 1

    @staticmethod
    def _sanitize_refs_array(refs_array: List[List[List[Any]]], lines: List[str], min_line: int, max_line: int) -> List[List[List[Any]]]:
        """
        refs 배열을 정리하고 유효성을 검사합니다.
        JavaScript의 _sanitizeRefsArray와 동일한 로직
        
        Args:
            refs_array: 정리할 refs 배열
            lines: 원본 텍스트의 줄 목록 (줄 번호 prefix 제거된 상태)
            min_line: 유효한 최소 줄 번호
            max_line: 유효한 최대 줄 번호
            
        Returns:
            정리된 refs 배열
        """
        def clamp(n: int, lo: int, hi: int) -> int:
            return max(lo, min(hi, n))
        
        def try_relocate(line: int, phrase: str) -> int:
            """문구가 해당 라인에 없으면 ±5 라인 탐색"""
            if not isinstance(phrase, str) or not phrase.strip():
                return line
            
            def has_phrase(ln: int) -> bool:
                idx = ln - min_line
                if 0 <= idx < len(lines):
                    content = lines[idx]
                    return phrase in content
                return False
            
            if has_phrase(line):
                return line
            
            # ±5 라인 내에서 탐색
            for d in range(1, 6):
                if line - d >= min_line and has_phrase(line - d):
                    return line - d
                if line + d <= max_line and has_phrase(line + d):
                    return line + d
            
            return line  # 못 찾으면 원래 라인 유지
        
        sanitized_array = []
        for mono in refs_array:
            if not mono or not isinstance(mono, list) or len(mono) != 2:
                sanitized_array.append(mono)
                continue
            
            s, e = mono
            if not isinstance(s, list) or len(s) != 2 or not isinstance(e, list) or len(e) != 2:
                sanitized_array.append(mono)
                continue

            # 문자열 숫자가 전달된 경우에 대한 추가 대응 로직
            if s[0] and isinstance(s[0], str) and s[0].isdigit():
                s[0] = int(s[0])
            if e[0] and isinstance(e[0], str) and e[0].isdigit():
                e[0] = int(e[0])
            
            s_line = s[0] if isinstance(s[0], int) else min_line
            e_line = e[0] if isinstance(e[0], int) else s_line
            s_phrase = s[1]
            e_phrase = e[1]
            
            # 라인 번호 클램핑
            s_line = clamp(s_line, min_line, max_line)
            e_line = clamp(e_line, min_line, max_line)
            
            # 문구 기반으로 라인 재배치
            s_line = try_relocate(s_line, s_phrase)
            e_line = try_relocate(e_line, e_phrase)
            
            # 시작/끝 라인 순서 보정
            if e_line < s_line:
                s_line, e_line = e_line, s_line
                s_phrase, e_phrase = e_phrase, s_phrase
            
            sanitized_array.append([[s_line, s_phrase], [e_line, e_phrase]])
        
        return sanitized_array

    @staticmethod
    def _clamp_refs_array(refs_array: List[List[List[Any]]], lines: List[str], min_line: int, max_line: int) -> List[List[List[int]]]:
        """
        refs 배열의 최종 인덱스 클램핑을 수행합니다.
        JavaScript의 _clampRefsArray와 동일한 로직
        
        Args:
            refs_array: 클램핑할 refs 배열
            lines: 원본 텍스트의 줄 목록 (줄 번호 prefix 제거된 상태)
            min_line: 유효한 최소 줄 번호
            max_line: 유효한 최대 줄 번호
            
        Returns:
            클램핑된 refs 배열
        """
        def clamp(n: int, lo: int, hi: int) -> int:
            return max(lo, min(hi, n))
        
        def get_line_length(ln: int) -> int:
            idx = ln - min_line
            if 0 <= idx < len(lines):
                content = lines[idx]
                return max(1, len(content))
            return 1
        
        clamped_array = []
        for mono in refs_array:
            if not mono or not isinstance(mono, list) or len(mono) != 2:
                clamped_array.append(mono)
                continue
            
            start_ref, end_ref = mono
            if (not isinstance(start_ref, list) or len(start_ref) != 2 or 
                not isinstance(end_ref, list) or len(end_ref) != 2):
                clamped_array.append(mono)
                continue
            
            s_line, s_col = start_ref
            e_line, e_col = end_ref
            
            # 라인 번호 클램핑
            s_line = clamp(s_line, min_line, max_line)
            e_line = clamp(e_line, min_line, max_line)
            
            # 칼럼 인덱스 클램핑
            s_col = max(1, min(get_line_length(s_line), s_col if isinstance(s_col, int) else 1))
            e_col = max(1, min(get_line_length(e_line), e_col if isinstance(e_col, int) else get_line_length(e_line)))
            
            # 시작/끝 위치 순서 보정
            if e_line < s_line or (e_line == s_line and e_col < s_col):
                ordered = [[e_line, e_col], [s_line, s_col]]
            else:
                ordered = [[s_line, s_col], [e_line, e_col]]
            
            clamped_array.append(ordered)
        
        return clamped_array
    
    @staticmethod
    def _filter_structural_and_zero_length(refs_array: List[List[List[Any]]], lines: List[str], state: State, log_prefix: str) -> List[List[List[Any]]]:
        """
        Stage 3.5: refs **재배치** (이전엔 drop 했으나 LLM 의 매핑 의도를 보존하기 위해
        본문 위치로 옮기는 방식으로 전환).

        처리:
          1) zero-length ref (start == end) — drop (의미 없음)
          2) markdown 헤더 (`#####` 등) 시작 ref —
             user story 헤더 (`##### [PROJ-US-FR-XXX]`) 면 그 user story 의 본문 위치로 옮기고
             그 외 섹션 헤더 (예: `## 1. 사용자 스토리 목록`) 는 drop.
          3) 표 row (`|`) 시작 ref —
             해당 row 에서 `[PROJ-US-FR-XXX]` 추출 → 같은 ID 의 `#####` 본문 위치로 이동.
             ID 없으면 drop.
          4) 구분선 (`---`) / 공백 라인 시작 ref — drop.
          5) 그 외 (본문 라인) 는 그대로 보존.

        결과: LLM 이 "이 element 는 FR-004 에 매핑" 이라고 헤더 / TOC row 를 anchor 로
        잡은 경우, anchor 위치만 본문 (As a narrative 또는 인수기준 블록) 으로 옮겨져서
        시각화 시 의미있는 highlight 가 됨.
        """
        if not refs_array or not lines:
            return refs_array

        # user story 본문 위치 인덱스 사전 계산: { 'PROJ-US-FR-001': narrative_line_num, ... }
        # narrative 라인 ('> *As a ...') 우선, 없으면 첫 content 라인.
        import re as _re
        us_index = {}
        us_header_re = _re.compile(r'^\s*#{4,6}\s+\[([A-Za-z][\w-]*US-(?:FR|NFR)-\d+)\]')
        narrative_re = _re.compile(r'^\s*>\s*\*?\s*As a')
        for i, ln in enumerate(lines):
            m = us_header_re.match(ln or '')
            if m:
                us_id = m.group(1)
                # 다음 같은 레벨 헤더 또는 `---` 까지가 body
                body_end = len(lines)
                for j in range(i+1, len(lines)):
                    nxt = (lines[j] or '').strip()
                    if not nxt: continue
                    if us_header_re.match(nxt) or nxt.startswith('---'):
                        body_end = j
                        break
                # narrative 우선
                narrative_line = None
                first_content_line = None
                for j in range(i+1, body_end):
                    raw = lines[j] or ''
                    stripped = raw.strip()
                    if not stripped: continue
                    if stripped.startswith('#'): continue
                    if stripped.startswith('|'): continue
                    if all(c in '- \t' for c in stripped) and len(stripped) >= 3: continue
                    if first_content_line is None:
                        first_content_line = j + 1  # 1-based
                    if narrative_re.match(raw):
                        narrative_line = j + 1
                        break
                target = narrative_line or first_content_line
                if target:
                    us_index[us_id] = target

        def _is_zero(s_line, s_col, e_line, e_col):
            return s_col is not None and e_col is not None and s_line == e_line and s_col == e_col

        def _line_classify(text):
            if text is None: return 'oob'
            stripped = text.strip()
            if not stripped: return 'empty'
            if stripped.startswith('#'): return 'header'
            if stripped.startswith('|'): return 'table'
            if all(c in '- \t' for c in stripped) and len(stripped) >= 3: return 'sep'
            return 'content'

        def _extract_us_id(text):
            if not text: return None
            m = _re.search(r'\[([A-Za-z][\w-]*US-(?:FR|NFR)-\d+)\]', text)
            return m.group(1) if m else None

        def _line_to_full_range(line_num):
            """1-based 라인 → 그 라인 전체를 cover 하는 ref"""
            if not (1 <= line_num <= len(lines)):
                return None
            content = lines[line_num - 1] or ''
            return [[line_num, 1], [line_num, max(1, len(content))]]

        dropped = 0
        relocated = 0
        result = []
        for mono in refs_array:
            if not isinstance(mono, list) or len(mono) != 2:
                result.append(mono)
                continue
            s, e = mono
            if not (isinstance(s, list) and len(s) == 2 and isinstance(e, list) and len(e) == 2):
                result.append(mono)
                continue
            try:
                s_line = int(s[0])
                s_col = int(s[1]) if isinstance(s[1], (int, float)) else None
                e_line = int(e[0])
                e_col = int(e[1]) if isinstance(e[1], (int, float)) else None
            except (TypeError, ValueError):
                result.append(mono)
                continue

            # 1) zero-length: drop
            if _is_zero(s_line, s_col, e_line, e_col):
                dropped += 1
                continue

            idx = s_line - 1
            if not (0 <= idx < len(lines)):
                result.append(mono)
                continue

            cls = _line_classify(lines[idx])

            if cls == 'content':
                # 본문 시작 — 그대로 보존
                result.append(mono)
                continue

            if cls in ('empty', 'sep'):
                dropped += 1
                continue

            # cls in ('header', 'table') → user story ID 추출 후 본문으로 relocate
            us_id = _extract_us_id(lines[idx])
            if us_id and us_id in us_index:
                target_line = us_index[us_id]
                new_ref = _line_to_full_range(target_line)
                if new_ref:
                    result.append(new_ref)
                    relocated += 1
                    continue

            # ID 추출 못했거나 본문 위치 매핑 실패 — drop
            dropped += 1

        if dropped > 0 or relocated > 0:
            try:
                LogUtil.add_info_log(state, f"{log_prefix} stage-3.5 refs: relocated={relocated}, dropped={dropped} (zero-length/non-user-story)")
            except Exception:
                pass
        return result

    @staticmethod
    def _apply_requirement_index_mapping(refs_array: List[List[List[Any]]], requirement_index_mapping: RequirementIndexMapping) -> List[List[List[int]]]:
        """
        requirement_index_mapping을 적용하여 refs 배열을 변환합니다.
        """
        mapped_array = []
        for mono in refs_array:
            if not mono or not isinstance(mono, list) or len(mono) != 2:
                mapped_array.append(mono)
                continue
            
            s, e = mono
            if not isinstance(s, list) or len(s) != 2 or not isinstance(e, list) or len(e) != 2:
                mapped_array.append(mono)
                continue
        
            s_line = s[0]
            e_line = e[0]

            if s_line and isinstance(s_line, int):
                s_line = requirement_index_mapping.get(s_line, s_line)
            if e_line and isinstance(e_line, int):
                e_line = requirement_index_mapping.get(e_line, e_line)
            
            mapped_array.append([[s_line, s[1]], [e_line, e[1]]])
        return mapped_array