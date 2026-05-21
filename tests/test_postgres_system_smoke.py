"""PostgresSystem 어댑터 스모크 테스트 (Phase 1 검증)

로컬 dev PostgreSQL(platform/data-gateway/docker-compose.dev.yml) 이 떠 있어야 한다.
실행: .venv/bin/python tests/test_postgres_system_smoke.py
"""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eventstorming_generator.systems.database.postgres_system import PostgresSystem  # noqa: E402

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}")


def main():
    db = PostgresSystem.initialize(
        host="localhost", port=5432, dbname="msaez", user="msaez", password="msaez_dev"
    )
    NS = "smoke_ns"

    # 정리
    for p in (f"jobs/{NS}/job1", f"jobs/{NS}/job2",
              f"requestedJobs/{NS}/req1", f"requestedJobs/{NS}/req2", f"requestedJobs/{NS}/req3",
              "jobStates/smoke_ns/s1"):
        db.delete_data(p)

    # 1. 전체 행 set/get
    db.set_data(f"jobs/{NS}/job1", {"state": {"inputs": {"a": 1}, "outputs": {}}})
    check("전체 행 set/get",
          db.get_data(f"jobs/{NS}/job1") == {"state": {"inputs": {"a": 1}, "outputs": {}}})

    # 2. 깊은 서브경로 set/get (리스트)
    db.set_data(f"jobs/{NS}/job1/state/outputs/logs", ["line1", "line2"])
    check("깊은 서브경로 set/get",
          db.get_data(f"jobs/{NS}/job1/state/outputs/logs") == ["line1", "line2"])

    # 3. 깊은 서브경로 스칼라
    db.set_data(f"jobs/{NS}/job1/state/outputs/isCompleted", True)
    check("깊은 서브경로 스칼라",
          db.get_data(f"jobs/{NS}/job1/state/outputs/isCompleted") is True)

    # 4. 서브경로 update(merge)
    db.update_data(f"jobs/{NS}/job1/state/outputs", {"isFailed": False})
    out = db.get_data(f"jobs/{NS}/job1/state/outputs")
    check("서브경로 update merge",
          out.get("isFailed") is False and out.get("isCompleted") is True
          and out.get("logs") == ["line1", "line2"])

    # 5. 전체 행 update(shallow merge)
    db.update_data(f"jobs/{NS}/job1", {"extra": "x"})
    v = db.get_data(f"jobs/{NS}/job1")
    check("전체 행 update merge", v.get("extra") == "x" and "state" in v)

    # 6. conditional_update — 변경분만 적용
    old = db.get_data(f"jobs/{NS}/job1")
    new = dict(old)
    new["extra"] = "y"
    db.conditional_update_data(f"jobs/{NS}/job1", new, old)
    check("conditional_update", db.get_data(f"jobs/{NS}/job1/extra") == "y")

    # 7. get_children — 컬렉션 조회
    db.set_data(f"jobs/{NS}/job2", {"state": {}})
    children = db.get_children_data(f"jobs/{NS}")
    check("get_children 컬렉션", children is not None and {"job1", "job2"} <= set(children.keys()))

    # 8. 서브경로 삭제
    db.delete_data(f"jobs/{NS}/job1/extra")
    check("서브경로 삭제", db.get_data(f"jobs/{NS}/job1/extra") is None)

    # 9. 전체 행 삭제
    db.delete_data(f"jobs/{NS}/job2")
    check("전체 행 삭제", db.get_data(f"jobs/{NS}/job2") is None)

    # 10. kv_store catch-all
    db.set_data("jobStates/smoke_ns/s1", {"status": "running"})
    check("kv set/get", db.get_data("jobStates/smoke_ns/s1") == {"status": "running"})
    check("kv 자식 조립", db.get_data("jobStates/smoke_ns") == {"s1": {"status": "running"}})

    # 11. SKIP LOCKED 큐 클레임 (순차)
    db.set_data(f"requestedJobs/{NS}/req1", {"inputs": {"x": 1}})
    db.set_data(f"requestedJobs/{NS}/req2", {"inputs": {"x": 2}})
    c1 = db.claim_pending_job(NS, "pod-A")
    c2 = db.claim_pending_job(NS, "pod-B")
    c3 = db.claim_pending_job(NS, "pod-C")
    claimed_ids = {c["job_id"] for c in (c1, c2) if c}
    check("SKIP LOCKED 서로 다른 2건 클레임", c1 and c2 and len(claimed_ids) == 2)
    check("SKIP LOCKED 대기 소진 시 None", c3 is None)

    # 12. SKIP LOCKED 동시성 — 5스레드가 3건을 다투어도 중복 클레임 없음
    db.set_data(f"requestedJobs/{NS}/req3", {"inputs": {"x": 3}})  # 1건 추가 (총 1건 pending: req3)
    results = []
    lock = threading.Lock()

    def worker(i):
        r = db.claim_pending_job(NS, f"t-{i}")
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    got = [r for r in results if r]
    check("동시성: 1건만 클레임 (중복 없음)",
          len(got) == 1 and got[0]["job_id"] == f"req3")

    # 정리
    for p in (f"jobs/{NS}/job1", f"requestedJobs/{NS}/req1",
              f"requestedJobs/{NS}/req2", f"requestedJobs/{NS}/req3",
              "jobStates/smoke_ns/s1"):
        db.delete_data(p)

    print(f"\n=== {PASS} passed, {FAIL} failed ===")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
