import io
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import create_app
from app.db import Database
from app.errors import LockUnavailableError, NotFoundError, ValidationError
from app.hll import REGISTER_COUNT, build_summary, hll_register

ZERO = [0] * REGISTER_COUNT


@pytest.fixture()
def db(tmp_path):
    return Database(str(tmp_path / "hll.sqlite3"))


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "web.sqlite3"))
    app.testing = True
    return app.test_client()


def regs(overrides=None):
    out = ZERO.copy()
    for position, value in (overrides or {}).items():
        out[position] = value
    return out


# ---------- 哈希口径 ----------

def test_hash_is_stable_and_in_range():
    position, rank = hll_register("reader-0001")
    assert 0 <= position < REGISTER_COUNT
    assert 1 <= rank <= 58
    # 同输入同输出，各馆可复现同一口径
    assert hll_register("reader-0001") == (position, rank)


def test_build_summary_is_idempotent():
    ids = [f"reader-{i}" for i in range(200)]
    s1 = build_summary(ids)
    s2 = build_summary(ids + ids)  # 重复读者不改变摘要
    assert s1 == s2
    assert len(s1) == REGISTER_COUNT
    assert max(s1) <= 58


# ---------- 校验 ----------

@pytest.mark.parametrize("bad_month", ["2026-13", "202611", "2026/11", "", 202611])
def test_bad_month_rejected(db, bad_month):
    with pytest.raises(ValidationError):
        db.upsert_report(bad_month, "ER-1", "LIB-A", ZERO)


def test_bad_identifier_rejected(db):
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER/1", "LIB-A", ZERO)


def test_registers_must_be_128_values_0_to_58(db):
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", [0] * 127)
    bad = ZERO.copy()
    bad[10] = 59
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", bad)
    bad[10] = True
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", bad)


# ---------- 报送、合并、换报、撤回 ----------

def test_merge_is_positionwise_max(db):
    a = regs({0: 5, 1: 3, 2: 10})
    b = regs({0: 8, 1: 1, 3: 20})
    c = regs({2: 15, 3: 12})

    r = db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    assert r["merged"] == a
    assert r["replaced"] is False

    r = db.upsert_report("2026-11", "ER-1", "LIB-B", b)
    assert r["replaced"] is False
    assert r["merged"] == regs({0: 8, 1: 3, 2: 10, 3: 20})

    r = db.upsert_report("2026-11", "ER-1", "LIB-C", c)
    assert r["merged"] == regs({0: 8, 1: 3, 2: 15, 3: 20})


def test_replace_report_increments_without_touching_others(db):
    a = regs({0: 5, 1: 9})
    b = regs({0: 8, 2: 4})
    db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    db.upsert_report("2026-11", "ER-1", "LIB-B", b)

    # A 换报：位置 0 从 5 降到 2（最大值仍为 B 的 8），位置 1 撤空，新增位置 4=30
    new_a = regs({0: 2, 4: 30})
    r = db.upsert_report("2026-11", "ER-1", "LIB-A", new_a)
    assert r["replaced"] is True
    assert r["merged"] == regs({0: 8, 2: 4, 4: 30})

    # B 的摘要原样保留
    assert db.get_report("2026-11", "ER-1", "LIB-B")["registers"] == b


def test_withdraw_recomputes_from_remaining_branches(db):
    a = regs({0: 5, 1: 9})
    b = regs({0: 8, 2: 4})
    db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    db.upsert_report("2026-11", "ER-1", "LIB-B", b)

    r = db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert r["merged"] == b

    with pytest.raises(NotFoundError):
        db.get_report("2026-11", "ER-1", "LIB-A")

    # 撤回最后一个馆：合并摘要全 0，但记录仍已保存可下载
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    merged = db.get_merged("2026-11", "ER-1")
    assert merged["registers"] == ZERO


def test_withdraw_missing_report_404(db):
    with pytest.raises(NotFoundError):
        db.withdraw_report("2026-11", "ER-1", "LIB-X")


def test_merged_404_when_never_reported(db):
    with pytest.raises(NotFoundError):
        db.get_merged("2026-11", "ER-9")


def test_different_months_and_resources_are_isolated(db):
    db.upsert_report("2026-10", "ER-1", "LIB-A", regs({0: 3}))
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 7}))
    db.upsert_report("2026-11", "ER-2", "LIB-A", regs({1: 11}))

    assert db.get_merged("2026-10", "ER-1")["registers"] == regs({0: 3})
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 7})
    assert db.get_merged("2026-11", "ER-2")["registers"] == regs({1: 11})


def test_tie_counts_allow_withdraw_to_reveal_next_max(db):
    # 两个馆在位置 0 同为 8，撤回一个后仍应保留 8；再撤回另一个才降为 0
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 8}))
    db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 8, 4: 6}))
    db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 8, 4: 6})
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    assert db.get_merged("2026-11", "ER-1")["registers"] == ZERO


def test_withdraw_max_reveals_second_largest(db):
    # 位置 0：A=10、B=5、C=3。撤回 A 后必须降为 B 的 5，而不是 0
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 10, 1: 7}))
    db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 5, 1: 2}))
    db.upsert_report("2026-11", "ER-1", "LIB-C", regs({0: 3}))
    db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5, 1: 2})

    # A 换报为更低值后，最大值同样应来自其余馆
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 1, 1: 9}))
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5, 1: 9})

    # 再撤回 B，位置 0 只剩 C 的 3
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 3, 1: 9})


def test_histogram_matches_branch_reports(db):
    # 增量直方图与独立重算的逐位置最大值始终一致（多轮随机换报/撤回）
    import random
    import sqlite3 as sqlite

    rng = random.Random(7)
    branches = [f"LIB-{i}" for i in range(6)]
    current = {b: ZERO.copy() for b in branches}
    for _ in range(60):
        branch = rng.choice(branches)
        if rng.random() < 0.7:
            new_regs = [rng.randint(0, 58) if rng.random() < 0.3 else 0
                        for _ in range(REGISTER_COUNT)]
            db.upsert_report("2026-11", "ER-1", branch, new_regs)
            current[branch] = new_regs
        else:
            try:
                db.withdraw_report("2026-11", "ER-1", branch)
                current[branch] = ZERO.copy()
            except NotFoundError:
                pass

    expected = [0] * REGISTER_COUNT
    for branch_regs in current.values():
        for i, v in enumerate(branch_regs):
            expected[i] = max(expected[i], v)
    assert db.get_merged("2026-11", "ER-1")["registers"] == expected

    # 直方图行数不随馆数膨胀：每位置最多 59 个非零取值
    conn = sqlite.connect(db.path)
    n_rows = conn.execute("SELECT COUNT(*) FROM register_counts").fetchone()[0]
    conn.close()
    assert n_rows <= REGISTER_COUNT * 58


# ---------- 原子性 ----------

def test_commit_failure_keeps_original_data(db, monkeypatch):
    from app.db import Database

    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))

    def failing_commit(conn):
        raise sqlite3.OperationalError("simulated commit failure")

    monkeypatch.setattr(Database, "_commit", staticmethod(failing_commit))
    with pytest.raises(sqlite3.OperationalError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 40}))
    monkeypatch.undo()

    # 馆报与合并摘要都沿用原数据
    assert db.get_report("2026-11", "ER-1", "LIB-A")["registers"] == regs({0: 5})
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5})


# ---------- 并发：每笔成功操作都计入汇总 ----------

def test_concurrent_different_branches_all_commit(db):
    branches = [f"LIB-{i:02d}" for i in range(8)]
    barrier = threading.Barrier(len(branches))

    def worker(branch_index):
        branch = branches[branch_index]
        # 每个馆在独立位置贡献不同值，最终合并结果应包含全部 8 笔
        registers = regs({branch_index: branch_index + 1})
        barrier.wait()
        db.upsert_report("2026-11", "ER-1", branch, registers)

    with ThreadPoolExecutor(max_workers=len(branches)) as pool:
        list(pool.map(worker, range(len(branches))))

    merged = db.get_merged("2026-11", "ER-1")["registers"]
    for i in range(8):
        assert merged[i] == i + 1

    # 每笔馆报均已落盘
    for i, branch in enumerate(branches):
        assert db.get_report("2026-11", "ER-1", branch)["registers"][i] == i + 1


def test_concurrent_replace_and_withdraw_converges(db):
    branches = [f"LIB-{i}" for i in range(6)]
    for i, branch in enumerate(branches):
        db.upsert_report("2026-11", "ER-1", branch, regs({0: 10 + i, 5: 3}))

    errors = []

    def replace_worker():
        try:
            for _ in range(5):
                db.upsert_report("2026-11", "ER-1", branches[0], regs({0: 50}))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def withdraw_worker(branch):
        try:
            db.withdraw_report("2026-11", "ER-1", branch)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(replace_worker)]
        futures += [pool.submit(withdraw_worker, b) for b in branches[1:]]
        for future in futures:
            future.result()

    assert not errors
    merged = db.get_merged("2026-11", "ER-1")["registers"]
    # 其余馆撤回后，只剩 LIB-0（换报为 50）；位置 5 全部撤回后为 0
    assert merged[0] == 50
    assert merged[5] == 0
    assert sum(merged) == 50


# ---------- HTTP 接口 ----------

def _payload(branch="LIB-A", reg_overrides=None):
    return {
        "month": "2026-11",
        "resource": "ER-1",
        "branch": branch,
        "registers": regs(reg_overrides),
    }


def test_http_submit_replace_and_download(client):
    r = client.post("/reports", json=_payload(reg_overrides={0: 5}))
    assert r.status_code == 201
    r = client.post("/reports", json=_payload(branch="LIB-B", reg_overrides={0: 9, 2: 4}))
    assert r.status_code == 201
    # 同馆再报 => 换报 200
    r = client.post("/reports", json=_payload(reg_overrides={0: 7}))
    assert r.status_code == 200

    r = client.get("/merged/2026-11/ER-1/download")
    assert r.status_code == 200
    assert r.headers["Content-Disposition"].startswith("attachment;")
    body = r.get_json()
    assert body["registers"] == regs({0: 9, 2: 4})
    assert len(body["registers"]) == REGISTER_COUNT


def test_http_upload_json_file_multipart(client):
    payload = _payload(reg_overrides={3: 12})
    data = {
        "month": "2026-11",
        "resource": "ER-1",
        "branch": "LIB-A",
        "file": (io.BytesIO(json.dumps(payload).encode()), "report.json"),
    }
    r = client.post("/reports", data=data, content_type="multipart/form-data")
    # 浏览器表单走 302 跳转
    assert r.status_code == 302
    r = client.get("/merged/2026-11/ER-1/download")
    assert r.get_json()["registers"] == regs({3: 12})


def test_http_validation_error_is_400_json(client):
    bad = _payload()
    bad["registers"] = [0] * 127
    r = client.post("/reports", json=bad)
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_http_withdraw_and_404(client):
    client.post("/reports", json=_payload(reg_overrides={1: 6}))
    r = client.post("/reports/withdraw", json={
        "month": "2026-11", "resource": "ER-1", "branch": "LIB-A"
    })
    assert r.status_code == 200
    assert r.get_json()["merged"][1] == 0

    r = client.delete("/reports/withdraw", query_string={
        "month": "2026-11", "resource": "ER-1", "branch": "LIB-A"
    })
    assert r.status_code == 404

    assert client.get("/merged/2026-11/ER-9/download").status_code == 404


def test_http_get_branch_report_and_health(client):
    client.post("/reports", json=_payload(reg_overrides={2: 8}))
    r = client.get("/reports/2026-11/ER-1/LIB-A")
    assert r.status_code == 200
    assert r.get_json()["registers"][2] == 8
    assert client.get("/health").status_code == 200
    assert client.get("/").status_code == 200


# ---------- 写锁争用：1s 入账窗口、2s 可重试失败、读取不被阻塞 ----------


class HeldWriteLock:
    """在独立连接上持有 BEGIN IMMEDIATE 写锁的测试夹具。"""

    def __init__(self, db: Database):
        self.db = db
        self.conn: sqlite3.Connection | None = None

    def __enter__(self) -> "HeldWriteLock":
        self.conn = self.db._connect(for_write=True)
        self.conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, *exc) -> None:
        assert self.conn is not None
        self.conn.rollback()
        self.conn.close()


def test_write_succeeds_when_lock_frees_within_one_second(db):
    # 只有一笔合法写请求争用写锁，锁在收到请求后 1 秒内释放 => 照常入账
    holder = db._connect(for_write=True)
    holder.execute("BEGIN IMMEDIATE")  # 先占住写锁

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            db.upsert_report, "2026-11", "ER-1", "LIB-A", regs({0: 7})
        )
        time.sleep(0.1)  # 确保写请求已进入锁等待
        started = time.monotonic()
        time.sleep(0.4)  # 在 1s 预算内释放写锁
        holder.rollback()
        holder.close()
        result = future.result()
        elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert result["merged"][0] == 7
    assert db.get_report("2026-11", "ER-1", "LIB-A")["registers"][0] == 7


@pytest.mark.parametrize(
    "operation",
    [
        lambda db: db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 7})),
        lambda db: db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 9})),  # 换报
        lambda db: db.withdraw_report("2026-11", "ER-1", "LIB-A"),
    ],
)
def test_contended_write_fails_within_two_seconds_and_changes_nothing(db, operation):
    # 先落一笔已提交数据作为基线
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))
    baseline_report = db.get_report("2026-11", "ER-1", "LIB-A")
    baseline_merged = db.get_merged("2026-11", "ER-1")

    with HeldWriteLock(db):
        started = time.monotonic()
        with pytest.raises(LockUnavailableError):
            operation(db)
        elapsed = time.monotonic() - started

    # 锁持续占用：必须在 2s 内返回失败
    assert elapsed < 2.0
    # 失败的请求不能入账，也不能改变合并摘要
    assert db.get_report("2026-11", "ER-1", "LIB-A") == baseline_report
    assert db.get_merged("2026-11", "ER-1") == baseline_merged


def test_sixteen_contending_writers_download_returns_last_commit_within_one_second(tmp_path):
    db = Database(str(tmp_path / "contend.sqlite3"))
    # 最近一次已提交的合并摘要基线（种子馆 LIB-00，与下面 16 个写馆不重名）
    db.upsert_report("2026-11", "ER-1", "LIB-00", regs({0: 42}))
    committed = db.get_merged("2026-11", "ER-1")
    assert committed["registers"][0] == 42

    branches = [f"LIB-{i:02d}" for i in range(1, 17)]  # LIB-01 … LIB-16
    barrier = threading.Barrier(len(branches) + 1)
    results: list[object] = []

    def writer(branch: str):
        barrier.wait()
        try:
            db.upsert_report("2026-11", "ER-1", branch, regs({1: 3}))
            results.append("ok")
        except LockUnavailableError:
            results.append("locked")

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(writer, b) for b in branches]
        barrier.wait()  # 16 笔写请求同时开抢写锁
        started = time.monotonic()
        # 数据员在争用最激烈时发起下载
        snapshot = db.get_merged("2026-11", "ER-1")
        elapsed = time.monotonic() - started
        for future in futures:
            future.result()

    assert elapsed < 1.0
    # 拿到的是最近一次已提交的合并 JSON（下载不被写锁阻塞）
    assert snapshot == committed
    # 写锁串行化：拿到锁的请求全部成功，没拿到的全部明确失败，没有静默丢请求
    assert len(results) == 16
    assert set(results) <= {"ok", "locked"}
    assert "ok" in results
    # 已入账的馆报与合并摘要保持一致（成功时馆报和合并摘要一起生效）
    merged = db.get_merged("2026-11", "ER-1")["registers"]
    assert merged[0] == 42  # 种子馆的贡献始终保留
    assert merged[1] == 3  # 至少一笔写请求入账
    for branch in branches:
        report = db.get_report("2026-11", "ER-1", branch)
        assert report["registers"][1] == 3
        assert report["registers"][0] == 0


def test_http_lock_contention_returns_503_retryable_within_two_seconds(tmp_path):
    app = create_app(str(tmp_path / "web-lock.sqlite3"))
    client = app.test_client()
    db: Database = app.config["DATABASE"]
    client.post("/reports", json=_payload(reg_overrides={0: 5}))

    with HeldWriteLock(db):
        started = time.monotonic()
        r = client.post("/reports", json=_payload(reg_overrides={0: 9}))
        elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert r.status_code == 503
    body = r.get_json()
    assert body["retryable"] is True
    assert "重试" in body["error"]
    assert r.headers["Retry-After"] == "1"

    # 撤回同样在 2s 内返回可重试失败
    with HeldWriteLock(db):
        started = time.monotonic()
        r = client.post(
            "/reports/withdraw",
            json={"month": "2026-11", "resource": "ER-1", "branch": "LIB-A"},
        )
        elapsed = time.monotonic() - started
    assert elapsed < 2.0
    assert r.status_code == 503
    assert r.get_json()["retryable"] is True

    # 数据未变
    assert client.get("/reports/2026-11/ER-1/LIB-A").get_json()["registers"][0] == 5
    assert client.get("/merged/2026-11/ER-1/download").get_json()["registers"][0] == 5


def test_http_lock_failure_html_form_redirects_with_retry_hint(tmp_path):
    app = create_app(str(tmp_path / "web-html.sqlite3"))
    client = app.test_client()
    db: Database = app.config["DATABASE"]

    with HeldWriteLock(db):
        r = client.post(
            "/reports/withdraw",
            data={"month": "2026-11", "resource": "ER-1", "branch": "LIB-A"},
        )
    assert r.status_code == 302
    location = r.headers["Location"]
    assert "status=503" in location
    page = client.get(location)
    assert page.status_code == 200
    assert "重试".encode() in page.data


def test_failed_request_does_not_open_a_write_transaction(db):
    # 锁等待失败时连接上不存在未提交事务，回滚/关闭都干净，库内无残留
    with HeldWriteLock(db):
        conn = db._connect(for_write=True)
        try:
            with pytest.raises(LockUnavailableError):
                db._transaction(conn)
            # 没有活动事务：再开一个只读语句不会报 "cannot start a transaction"
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode.lower() == "wal"
        finally:
            conn.close()


def test_custom_lock_timeout_is_honored(tmp_path):
    # 更短的锁等待预算 => 更快失败，但仍是可重试的 LockUnavailableError
    fast = Database(str(tmp_path / "fast.sqlite3"), lock_timeout=0.3)
    fast.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))
    with HeldWriteLock(fast):
        started = time.monotonic()
        with pytest.raises(LockUnavailableError):
            fast.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 9}))
        elapsed = time.monotonic() - started
    assert 0.3 <= elapsed < 1.0
    # 数据保持基线
    assert fast.get_merged("2026-11", "ER-1")["registers"][0] == 5


def test_retry_after_failure_then_succeeds(db):
    # 模拟客户端重试：锁占用期失败，释放后原样重发即成功入账
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))
    holder = db._connect(for_write=True)
    holder.execute("BEGIN IMMEDIATE")
    with pytest.raises(LockUnavailableError):
        db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 9}))
    holder.rollback()
    holder.close()
    # 释放后重试同一请求
    result = db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 9}))
    assert result["merged"][0] == 9
    assert db.get_report("2026-11", "ER-1", "LIB-B")["registers"][0] == 9


def test_download_and_branch_read_unaffected_by_write_lock(db):
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5, 2: 8}))
    committed = db.get_merged("2026-11", "ER-1")
    with HeldWriteLock(db):
        started = time.monotonic()
        merged = db.get_merged("2026-11", "ER-1")
        report = db.get_report("2026-11", "ER-1", "LIB-A")
        elapsed = time.monotonic() - started
    # 只读路径不被写锁阻塞，远在 1s 内返回已提交快照
    assert elapsed < 1.0
    assert merged == committed
    assert report["registers"][2] == 8
