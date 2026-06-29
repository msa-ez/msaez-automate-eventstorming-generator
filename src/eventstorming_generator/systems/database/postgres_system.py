"""PostgreSQL Database 시스템 구현 (v1.0.30)

AceBase(경로 트리 + HTTP) 를 대체한다. 설계 근거: DB-migration-plan.md §4·§5.

핵심 모델 — "행 식별 + value 내부 JSON 경로":
    AceBase 경로  jobs/{ns}/{jobId}/state/outputs/logs
      -> 테이블 jobs, 행 (job_id={jobId}, namespace={ns})
      -> value 내부 JSON 경로 {state, outputs, logs}

알려진 핫 prefix 는 전용 테이블, 그 외 경로는 kv_store catch-all 로 라우팅된다.
"""
import os
import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from functools import partial
from typing import Dict, Any, Optional, Callable, List

try:
    import psycopg
    from psycopg.types.json import Jsonb
    from psycopg_pool import ConnectionPool
except ImportError:  # 의존성 미설치 시 import 단계에서 죽지 않도록
    psycopg = None
    Jsonb = None
    ConnectionPool = None

from ...utils.logging_util import LoggingUtil
from .database_system import DatabaseSystem


@dataclass
class _Route:
    """경로 라우팅 결과."""
    kind: str                       # 'row' | 'collection' | 'kv'
    table: str = ""                 # 정규화 테이블명 (kv 면 'kv_store')
    pk: Dict[str, str] = field(default_factory=dict)        # 행 식별 키 컬럼 값
    filters: Dict[str, str] = field(default_factory=dict)   # collection WHERE 조건
    insert_cols: Dict[str, str] = field(default_factory=dict)  # INSERT 시 추가 컬럼
    subpath: List[str] = field(default_factory=list)        # value 내부 JSON 경로
    key_col: str = ""               # collection 결과 dict 의 키가 될 컬럼
    path: str = ""                  # kv_store 원본 경로


# 테이블별 PK 컬럼 정의 (라우팅·SQL 생성에 사용)
_TABLE_PK = {
    "jobs": ["job_id"],
    "requested_jobs": ["job_id"],
    "definitions": ["project_id"],
    "user_lists": ["uid", "list_type", "project_id"],
    "definition_queue": ["project_id", "seq_key"],
    "definition_snapshots": ["project_id", "snapshot_key"],
    "users": ["uid"],
}


class PostgresSystem(DatabaseSystem):
    """PostgreSQL Database 시스템 구현 (싱글톤)."""

    _instance: Optional["PostgresSystem"] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str = None, port: int = None, dbname: str = None,
                 user: str = None, password: str = None):
        if self._initialized:
            return
        if psycopg is None:
            raise RuntimeError(
                "psycopg 가 설치되어 있지 않습니다. pyproject.toml 의 의존성을 확인하세요 "
                "(psycopg[binary], psycopg-pool)."
            )
        if host is None or port is None or dbname is None:
            raise ValueError("host, port, dbname 은 필수 매개변수입니다.")

        self.host = host
        self.port = port
        self.dbname = dbname
        conninfo = (
            f"host={host} port={port} dbname={dbname} "
            f"user={user or ''} password={password or ''}"
        )
        # 커넥션 풀 — psycopg_pool 은 스레드 안전. async 래퍼가 스레드에서 호출해도 안전하다.
        self._pool = ConnectionPool(conninfo, min_size=1, max_size=10, open=False)
        self._pool.open(wait=True, timeout=10)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self._initialized = True
        LoggingUtil.info("postgres_system", f"PostgreSQL 연결 초기화: {host}:{port}/{dbname}")

    @classmethod
    def initialize(cls, host: str = None, port: int = None, dbname: str = None,
                   user: str = None, password: str = None) -> "PostgresSystem":
        """싱글톤 인스턴스 초기화."""
        if cls._instance is None or not cls._instance._initialized:
            cls._instance = cls(host, port, dbname, user, password)
        return cls._instance

    @classmethod
    def instance(cls) -> "PostgresSystem":
        """초기화된 싱글톤 인스턴스 반환."""
        if cls._instance is None or not cls._instance._initialized:
            raise RuntimeError(
                "PostgresSystem 이 초기화되지 않았습니다. 먼저 PostgresSystem.initialize() 를 호출하세요."
            )
        return cls._instance

    # =========================================================================
    # 경로 라우팅
    # =========================================================================
    def _route(self, path: str) -> _Route:
        """AceBase 경로를 (테이블, 행 키, JSON 서브경로) 로 변환한다."""
        segs = [s for s in (path or "").strip("/").split("/") if s != ""]
        if not segs:
            return _Route(kind="kv", table="kv_store", path="")

        head = segs[0]

        # jobs/{ns}/{jobId}[/...subpath]   ·   requestedJobs/{ns}/{jobId}[/...subpath]
        if head in ("jobs", "requestedJobs"):
            table = "jobs" if head == "jobs" else "requested_jobs"
            if len(segs) == 1:
                return _Route(kind="collection", table=table, key_col="job_id")
            namespace = segs[1]
            if len(segs) == 2:
                return _Route(kind="collection", table=table,
                              filters={"namespace": namespace}, key_col="job_id")
            return _Route(kind="row", table=table,
                          pk={"job_id": segs[2]},
                          insert_cols={"namespace": namespace},
                          subpath=segs[3:])

        # definitions/{pid}/queue/{seqKey}  ·  definitions/{pid}/snapshotLists/{snapKey}
        # definitions/{pid}[/...subpath]
        if head == "definitions":
            if len(segs) == 1:
                return _Route(kind="collection", table="definitions", key_col="project_id")
            pid = segs[1]
            if len(segs) >= 3 and segs[2] == "queue":
                if len(segs) == 3:
                    return _Route(kind="collection", table="definition_queue",
                                  filters={"project_id": pid}, key_col="seq_key")
                return _Route(kind="row", table="definition_queue",
                              pk={"project_id": pid, "seq_key": segs[3]},
                              subpath=segs[4:])
            if len(segs) >= 3 and segs[2] == "snapshotLists":
                if len(segs) == 3:
                    return _Route(kind="collection", table="definition_snapshots",
                                  filters={"project_id": pid}, key_col="snapshot_key")
                return _Route(kind="row", table="definition_snapshots",
                              pk={"project_id": pid, "snapshot_key": segs[3]},
                              subpath=segs[4:])
            return _Route(kind="row", table="definitions",
                          pk={"project_id": pid}, subpath=segs[2:])

        # userLists/{uid}/{type}/{pid}[/...subpath]
        if head == "userLists":
            if len(segs) == 3:
                return _Route(kind="collection", table="user_lists",
                              filters={"uid": segs[1], "list_type": segs[2]},
                              key_col="project_id")
            if len(segs) >= 4:
                return _Route(kind="row", table="user_lists",
                              pk={"uid": segs[1], "list_type": segs[2], "project_id": segs[3]},
                              subpath=segs[4:])
            # userLists/{uid} 등 더 얕은 경로는 게이트웨이 영역 — kv 폴백
            return _Route(kind="kv", table="kv_store", path=path.strip("/"))

        # users/{uid}[/...subpath]
        if head == "users" and len(segs) >= 2:
            return _Route(kind="row", table="users",
                          pk={"uid": segs[1]}, subpath=segs[2:])

        # 그 외 모든 경로 → kv_store catch-all (jobStates/*, enrolledUsers/* 등)
        return _Route(kind="kv", table="kv_store", path=path.strip("/"))

    # =========================================================================
    # JSON 서브경로 헬퍼 (순수 Python — jsonb_set 의 깊은 부모 미생성 문제 회피)
    # =========================================================================
    @staticmethod
    def _nested_get(root: Any, subpath: List[str]) -> Any:
        cur = root
        for k in subpath:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return None
        return cur

    @staticmethod
    def _nested_set(root: Dict[str, Any], subpath: List[str], value: Any) -> None:
        cur = root
        for k in subpath[:-1]:
            if not isinstance(cur.get(k), dict):
                cur[k] = {}
            cur = cur[k]
        cur[subpath[-1]] = value

    @staticmethod
    def _nested_merge(root: Dict[str, Any], subpath: List[str], value: Dict[str, Any]) -> None:
        """subpath 위치의 dict 에 value 를 shallow merge (AceBase update 의미)."""
        cur = root
        for k in subpath:
            if not isinstance(cur.get(k), dict):
                cur[k] = {}
            cur = cur[k]
        if isinstance(value, dict):
            cur.update(value)

    @staticmethod
    def _nested_del(root: Dict[str, Any], subpath: List[str]) -> None:
        cur = root
        for k in subpath[:-1]:
            if not isinstance(cur.get(k), dict):
                return
            cur = cur[k]
        cur.pop(subpath[-1], None)

    # =========================================================================
    # 공통 실행 래퍼
    # =========================================================================
    def _safe(self, op_name: str, fn: Callable, default: Any) -> Any:
        try:
            return fn()
        except (OSError, ValueError, TypeError, LookupError, AttributeError, RuntimeError, ImportError, ArithmeticError, AssertionError, StopIteration, StopAsyncIteration, BufferError) as e:  # noqa: BLE001
            LoggingUtil.exception("postgres_system", f"{op_name} 실패", e)
            return default

    # =========================================================================
    # set_data — 경로에 값을 저장 (전체 교체)
    # =========================================================================
    def set_data(self, path: str, data: Any) -> bool:
        def _op():
            r = self._route(path)
            if r.kind == "kv":
                return self._kv_write(r.path, data, merge=False)
            if r.kind == "collection":
                # 컬렉션 전체 교체는 위험 — 지원하지 않음
                LoggingUtil.warning("postgres_system", f"set_data 컬렉션 경로 미지원: {path}")
                return False
            if r.subpath:
                return self._row_subpath_write(r, data, mode="set")
            return self._row_write(r, data, merge=False)
        return self._safe("데이터 업로드", _op, False)

    # =========================================================================
    # update_data — 경로의 데이터를 부분 병합
    # =========================================================================
    def update_data(self, path: str, data: Any) -> bool:
        def _op():
            r = self._route(path)
            if r.kind == "kv":
                return self._kv_write(r.path, data, merge=True)
            if r.kind == "collection":
                LoggingUtil.warning("postgres_system", f"update_data 컬렉션 경로 미지원: {path}")
                return False
            if r.subpath:
                return self._row_subpath_write(r, data, mode="merge")
            return self._row_write(r, data, merge=True)
        return self._safe("데이터 업데이트", _op, False)

    # =========================================================================
    # get_data — 경로의 데이터 조회
    # =========================================================================
    def get_data(self, path: str) -> Optional[Any]:
        def _op():
            r = self._route(path)
            if r.kind == "kv":
                return self._kv_read(r.path)
            if r.kind == "collection":
                return self._collection_read(r)
            value = self._row_read(r)
            if value is None:
                return None
            if r.subpath:
                return self._nested_get(value, r.subpath)
            return value
        return self._safe("데이터 조회", _op, None)

    # =========================================================================
    # delete_data — 경로의 데이터 삭제
    # =========================================================================
    def delete_data(self, path: str) -> bool:
        def _op():
            r = self._route(path)
            if r.kind == "kv":
                return self._kv_delete(r.path)
            if r.kind == "collection":
                LoggingUtil.warning("postgres_system", f"delete_data 컬렉션 경로 미지원: {path}")
                return False
            if r.subpath:
                return self._row_subpath_delete(r)
            return self._row_delete(r)
        return self._safe("데이터 삭제", _op, False)

    # =========================================================================
    # conditional_update_data — 변경된 부분만 적용 (AceBase 동작과 동일)
    # =========================================================================
    def conditional_update_data(self, path: str, data_to_update: Dict[str, Any],
                                previous_data: Dict[str, Any]) -> bool:
        updates = self._find_data_differences(data_to_update, previous_data)
        if not updates:
            return True
        LoggingUtil.info("postgres_system",
                         f"[conditional_update] path={path} diff_count={len(updates)}")
        for rel_path, value in updates.items():
            full_path = f"{path}/{rel_path}" if path else rel_path
            if value is None:
                ok = self.delete_data(full_path)
            else:
                ok = self.set_data(full_path, value)
            if ok is False:
                return False
        return True

    # =========================================================================
    # get_children_data — 자식 노드 조회
    # =========================================================================
    def get_children_data(self, path: str) -> Optional[Dict[str, Dict[str, Any]]]:
        data = self.get_data(path)
        if data is None or not isinstance(data, dict):
            return None
        return data

    # =========================================================================
    # 정규화 테이블 — 행 단위 read/write
    # =========================================================================
    def _row_read(self, r: _Route) -> Optional[Any]:
        where, params = self._pk_where(r)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {r.table} WHERE {where}", params)
            row = cur.fetchone()
            return row[0] if row else None

    def _row_write(self, r: _Route, data: Any, merge: bool) -> bool:
        cols = list(r.pk.keys()) + list(r.insert_cols.keys()) + ["value"]
        vals = list(r.pk.values()) + list(r.insert_cols.values()) + [Jsonb(data)]
        placeholders = ", ".join(["%s"] * len(cols))
        conflict = ", ".join(_TABLE_PK[r.table])
        if merge:
            set_clause = f"value = {r.table}.value || EXCLUDED.value, updated_at = now()"
        else:
            set_clause = "value = EXCLUDED.value, updated_at = now()"
        sql = (
            f"INSERT INTO {r.table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
        )
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, vals)
        return True

    def _row_subpath_write(self, r: _Route, data: Any, mode: str) -> bool:
        """value 내부 JSON 경로에 set/merge. 행 잠금 후 read-modify-write (원자적)."""
        where, params = self._pk_where(r)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {r.table} WHERE {where} FOR UPDATE", params)
            row = cur.fetchone()
            value = row[0] if row and isinstance(row[0], dict) else {}
            if mode == "merge":
                self._nested_merge(value, r.subpath, data)
            else:
                self._nested_set(value, r.subpath, data)
            if row:
                cur.execute(
                    f"UPDATE {r.table} SET value = %s, updated_at = now() WHERE {where}",
                    [Jsonb(value)] + params,
                )
            else:
                cols = list(r.pk.keys()) + list(r.insert_cols.keys()) + ["value"]
                vals = list(r.pk.values()) + list(r.insert_cols.values()) + [Jsonb(value)]
                placeholders = ", ".join(["%s"] * len(cols))
                cur.execute(
                    f"INSERT INTO {r.table} ({', '.join(cols)}) VALUES ({placeholders})",
                    vals,
                )
        return True

    def _row_delete(self, r: _Route) -> bool:
        where, params = self._pk_where(r)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {r.table} WHERE {where}", params)
        return True

    def _row_subpath_delete(self, r: _Route) -> bool:
        """value 내부 JSON 경로의 키를 제거 (#- 연산자)."""
        where, params = self._pk_where(r)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {r.table} SET value = value #- %s, updated_at = now() WHERE {where}",
                [list(r.subpath)] + params,
            )
        return True

    def _collection_read(self, r: _Route) -> Optional[Dict[str, Any]]:
        """컬렉션(네임스페이스/프로젝트 단위) 의 모든 행을 {키: value} 로 반환."""
        # jobs / requested_jobs 의 루트 컬렉션(namespace 미지정)은 AceBase 트리와
        # 동일하게 {namespace: {job_id: value}} 2단계 구조로 반환한다 (job manager 가
        # 루트를 namespace 별로 조회하기 때문 — 평면이면 잡을 못 찾는다).
        if not r.filters and r.table in ("jobs", "requested_jobs"):
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(f"SELECT namespace, {r.key_col}, value FROM {r.table}")
                rows = cur.fetchall()
            if not rows:
                return None
            nested: Dict[str, Any] = {}
            for ns, key, value in rows:
                nested.setdefault(ns, {})[key] = value
            return nested

        if r.filters:
            where = " AND ".join(f"{c} = %s" for c in r.filters)
            params = list(r.filters.values())
        else:
            where, params = "TRUE", []
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT {r.key_col}, value FROM {r.table} WHERE {where}", params)
            rows = cur.fetchall()
        if not rows:
            return None
        return {row[0]: row[1] for row in rows}

    def restore_data_from_storage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # job manager(atomic_claim_job)가 복원본을 변형한 뒤 원본(current_data)과
        # diff 한다. passthrough 면 restored_data 가 current_data 와 동일 객체가 되어
        # diff 가 비어 클레임 write 가 누락된다 → 원본과 독립된 깊은 복사본을 반환.
        import copy
        return copy.deepcopy(data) if data is not None else data

    @staticmethod
    def _pk_where(r: _Route):
        where = " AND ".join(f"{c} = %s" for c in r.pk)
        return where, list(r.pk.values())

    # =========================================================================
    # kv_store — catch-all (경로를 평면 키로 저장, 자식은 prefix 로 조립)
    # =========================================================================
    def _kv_write(self, path: str, data: Any, merge: bool) -> bool:
        if merge:
            set_clause = "value = kv_store.value || EXCLUDED.value, updated_at = now()"
        else:
            set_clause = "value = EXCLUDED.value, updated_at = now()"
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO kv_store (path, value) VALUES (%s, %s) "
                f"ON CONFLICT (path) DO UPDATE SET {set_clause}",
                [path, Jsonb(data)],
            )
        return True

    def _kv_read(self, path: str) -> Optional[Any]:
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT value FROM kv_store WHERE path = %s", [path])
            exact = cur.fetchone()
            cur.execute(
                "SELECT path, value FROM kv_store WHERE path LIKE %s",
                [path + "/%"],
            )
            children = cur.fetchall()
        if not children:
            return exact[0] if exact else None
        # 자식들을 중첩 dict 로 조립
        assembled: Dict[str, Any] = {}
        for child_path, value in children:
            rel = child_path[len(path) + 1:].split("/")
            self._nested_set(assembled, rel, value)
        if exact and isinstance(exact[0], dict):
            merged = dict(exact[0])
            merged.update(assembled)
            return merged
        return assembled

    def _kv_delete(self, path: str) -> bool:
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM kv_store WHERE path = %s OR path LIKE %s",
                [path, path + "/%"],
            )
        return True

    # =========================================================================
    # LLM Job 큐 — SKIP LOCKED 클레임 (DatabaseSystem 인터페이스 외 추가 메서드)
    # =========================================================================
    def claim_pending_job(self, namespace: str, pod_id: str) -> Optional[Dict[str, Any]]:
        """대기 중인 requested_job 하나를 원자적으로 점유한다 (§4.3).

        Returns:
            {job_id, value} 또는 대기 작업이 없으면 None.
        """
        def _op():
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE requested_jobs SET status = 'processing', "
                    "locked_by = %s, locked_at = now() "
                    "WHERE job_id = ("
                    "  SELECT job_id FROM requested_jobs "
                    "  WHERE namespace = %s AND status = 'pending' "
                    "  ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) "
                    "RETURNING job_id, value",
                    [pod_id, namespace],
                )
                row = cur.fetchone()
                return {"job_id": row[0], "value": row[1]} if row else None
        return self._safe("Job 큐 클레임", _op, None)

    def mark_job_status(self, job_id: str, status: str) -> bool:
        """requested_job 의 status 를 갱신한다 (completed/failed 등)."""
        def _op():
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE requested_jobs SET status = %s WHERE job_id = %s",
                    [status, job_id],
                )
            return True
        return self._safe("Job status 갱신", _op, False)

    # =========================================================================
    # 비동기 래퍼 (base 에 없는 것 보강)
    # =========================================================================
    async def _run(self, fn: Callable, *args) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, partial(fn, *args))

    async def set_data_async(self, path: str, data: Any) -> bool:
        return await self._run(self.set_data, path, data)

    async def conditional_update_data_async(self, path: str, data_to_update: Dict[str, Any],
                                            previous_data: Dict[str, Any]) -> bool:
        return await self._run(self.conditional_update_data, path, data_to_update, previous_data)

    # =========================================================================
    # watch — PostgreSQL 직결 어댑터에서는 미사용 (실시간은 게이트웨이 담당)
    # =========================================================================
    def watch_data(self, path: str, callback: Callable[[Optional[Dict[str, Any]]], None]) -> bool:
        LoggingUtil.warning("postgres_system",
                            f"watch_data 는 PostgreSQL 어댑터에서 지원하지 않습니다: {path}")
        return False

    def unwatch_data(self, path: str) -> bool:
        LoggingUtil.warning("postgres_system",
                            f"unwatch_data 는 PostgreSQL 어댑터에서 지원하지 않습니다: {path}")
        return False
