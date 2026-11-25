"""SQLite 存档与合并维护。

表结构
------
branch_reports
    各馆按 (月份, 资源号, 馆号) 保存的现行 HLL 摘要（128 个寄存器）。
    换报即整行覆盖，撤回即删除。

register_counts
    按 (月份, 资源号, 寄存器位置, 取值) 维护该取值当前有多少个馆贡献，
    即每个位置的完整取值直方图。它是各馆现行摘要逐位置最大值的增量
    表示：换报/撤回时只需读取本馆旧摘要做差分，按主键增减对应取值的
    计数，再以 GROUP BY 位置取 MAX(value) 得到合并值，全程不遍历其他
    馆的摘要。最大值馆撤回或降值时，次大值可直接从直方图恢复。

merged_summaries
    按 (月份, 资源号) 保存的合并摘要，与馆报在同一事务内一并生效。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from .errors import LockUnavailableError, NotFoundError
from .hll import REGISTER_COUNT, validate_identifier, validate_month, validate_registers

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS branch_reports (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    branch     TEXT NOT NULL,
    registers  TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource, branch)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS register_counts (
    month     TEXT NOT NULL,
    resource  TEXT NOT NULL,
    position  INTEGER NOT NULL,
    value     INTEGER NOT NULL,
    cnt       INTEGER NOT NULL,
    PRIMARY KEY (month, resource, position, value)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS merged_summaries (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    registers  TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource)
) WITHOUT ROWID;
"""

# 写锁等待上限：收到写请求后，BEGIN IMMEDIATE 在此时间内拿到锁就照常入账；
# 超时则抛 LockUnavailableError（HTTP 503，可重试），全程不开启写事务。
DEFAULT_LOCK_TIMEOUT = 1.0
# 读连接的 busy_timeout：WAL 下读基本不被写阻塞，仅在 checkpoint 等罕见
# 场景短暂等待，0.5s 足以保证下载远在 1s 预算内返回最近一次已提交快照。
READ_BUSY_TIMEOUT = 0.5
# 单次 BEGIN IMMEDIATE 在内核 busy handler 内的等待粒度；外层用 deadline 兜底。
_LOCK_POLL_GRANULARITY = 0.25
_BUSY_ERROR_MSGS = ("database is locked", "database table is locked", "database schema is locked")


class Database:
    def __init__(self, path: str, lock_timeout: float = DEFAULT_LOCK_TIMEOUT):
        self.path = path
        # 写锁等待上限可由部署方按环境调整，但语义不变：超时即可重试的失败。
        self.lock_timeout = float(lock_timeout)
        self._init_lock = threading.Lock()
        self._initialized = False
        self._ensure_schema()

    def _connect(self, *, for_write: bool) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            # 外层自行用 deadline 控制等待，sqlite3.connect 的 timeout 只是兜底。
            timeout=self.lock_timeout if for_write else READ_BUSY_TIMEOUT,
            isolation_level=None,  # 事务完全由显式 BEGIN/COMMIT 控制
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        # WAL 模式在 _ensure_schema 中一次性落库（持久属性），每个请求连接
        # 不再重复切换 journal_mode，避免读连接在写锁占用期做额外元数据操作。
        if for_write:
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            conn = self._connect(for_write=True)
            try:
                conn.executescript(SCHEMA)
            finally:
                conn.close()
            self._initialized = True

    # -- 事务工具 -------------------------------------------------------

    @staticmethod
    def _commit(conn: sqlite3.Connection) -> None:
        """单独抽出便于测试注入提交失败；正常路径即 SQLite 提交。"""
        conn.commit()

    @staticmethod
    def _is_busy(exc: sqlite3.OperationalError) -> bool:
        message = str(exc).lower()
        return any(token in message for token in _BUSY_ERROR_MSGS)

    def _transaction(self, conn: sqlite3.Connection) -> sqlite3.Cursor:
        """开启写事务（BEGIN IMMEDIATE），写锁等待严格限定在 lock_timeout 内。

        - 锁在预算内释放：立刻拿到锁并返回，请求照常入账；
        - 预算耗尽仍被占用：抛 LockUnavailableError。此时从未成功开启事务，
          库内数据与合并摘要完全不变，调用方可安全重试。
        """
        deadline = time.monotonic() + self.lock_timeout
        last_error: sqlite3.OperationalError | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            # 让 SQLite 内核先等一小段，避免忙等占满 GIL；单次等待不超过剩余预算。
            conn.execute(f"PRAGMA busy_timeout = {int(min(remaining, _LOCK_POLL_GRANULARITY) * 1000)}")
            try:
                return conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if not self._is_busy(exc):
                    raise
                last_error = exc
        assert last_error is not None
        raise LockUnavailableError(
            f"数据库写锁在 {self.lock_timeout:.0f} 秒内一直被占用，本次操作未入账，请稍后重试"
        )

    # -- 内部：基于本馆旧摘要的差分更新 ----------------------------------

    @staticmethod
    def _apply_deltas(
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        deltas: dict[int, dict[int, int]],
    ) -> None:
        """对逐位置取值直方图施加 {位置: {取值: 增减量}} 的差分。

        只按 (月份,资源,位置,取值) 主键 upsert/delete，不读取其他馆摘要。
        """
        for position, changes in deltas.items():
            for value, delta in changes.items():
                if value == 0:
                    continue  # 取值 0 不占用计数，全馆无贡献时该位置自然为 0
                row = conn.execute(
                    "SELECT cnt FROM register_counts "
                    "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                    (month, resource, position, value),
                ).fetchone()
                new_cnt = (row["cnt"] if row is not None else 0) + delta
                if new_cnt <= 0:
                    conn.execute(
                        "DELETE FROM register_counts "
                        "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                        (month, resource, position, value),
                    )
                elif row is None:
                    conn.execute(
                        "INSERT INTO register_counts "
                        "(month, resource, position, value, cnt) VALUES (?, ?, ?, ?, ?)",
                        (month, resource, position, value, new_cnt),
                    )
                else:
                    conn.execute(
                        "UPDATE register_counts SET cnt = ? "
                        "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                        (new_cnt, month, resource, position, value),
                    )

    @staticmethod
    def _refresh_merged(
        conn: sqlite3.Connection, month: str, resource: str
    ) -> list[int]:
        """从取值直方图聚合（逐位置 MAX）并落盘合并摘要，与馆报同事务生效。"""
        merged = [0] * REGISTER_COUNT
        rows = conn.execute(
            "SELECT position, MAX(value) AS value FROM register_counts "
            "WHERE month = ? AND resource = ? GROUP BY position",
            (month, resource),
        ).fetchall()
        for row in rows:
            merged[row["position"]] = row["value"]
        payload = json.dumps(merged, separators=(",", ":"))
        conn.execute(
            "INSERT INTO merged_summaries (month, resource, registers, updated_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT (month, resource) DO UPDATE SET "
            "registers = excluded.registers, updated_at = excluded.updated_at",
            (month, resource, payload),
        )
        return merged

    # -- 对外操作 --------------------------------------------------------

    def upsert_report(
        self, month: str, resource: str, branch: str, registers: list[int]
    ) -> dict[str, Any]:
        """上传或换报：整行覆盖本馆摘要，并增量维护合并结果。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)
        registers = validate_registers(registers)

        conn = self._connect(for_write=True)
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                old_registers = (
                    json.loads(old_row["registers"]) if old_row is not None else None
                )

                deltas: dict[int, dict[int, int]] = {}
                for position, new_value in enumerate(registers):
                    old_value = (
                        old_registers[position] if old_registers is not None else 0
                    )
                    if old_value == new_value:
                        continue
                    change: dict[int, int] = {}
                    if old_value:
                        change[old_value] = -1
                    if new_value:
                        change[new_value] = change.get(new_value, 0) + 1
                    if change:
                        deltas[position] = change
                if deltas:
                    self._apply_deltas(conn, month, resource, deltas)

                conn.execute(
                    "INSERT INTO branch_reports (month, resource, branch, registers, updated_at) "
                    "VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
                    "ON CONFLICT (month, resource, branch) DO UPDATE SET "
                    "registers = excluded.registers, updated_at = excluded.updated_at",
                    (
                        month,
                        resource,
                        branch,
                        json.dumps(registers, separators=(",", ":")),
                    ),
                )
                merged = self._refresh_merged(conn, month, resource)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": registers,
            "merged": merged,
            "replaced": old_registers is not None,
        }

    def withdraw_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """撤回本馆摘要：删除本馆行并增量维护合并结果，不触碰其他馆摘要。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect(for_write=True)
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                if old_row is None:
                    raise NotFoundError(
                        f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送，无法撤回"
                    )
                old_registers = json.loads(old_row["registers"])

                deltas: dict[int, dict[int, int]] = {}
                for position, old_value in enumerate(old_registers):
                    if old_value:
                        deltas[position] = {old_value: -1}
                if deltas:
                    self._apply_deltas(conn, month, resource, deltas)

                conn.execute(
                    "DELETE FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                )
                merged = self._refresh_merged(conn, month, resource)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "merged": merged,
        }

    def get_merged(self, month: str, resource: str) -> dict[str, Any]:
        """读取已保存的合并摘要（无任何报送记录时 404）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)

        conn = self._connect(for_write=False)
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM merged_summaries "
                "WHERE month = ? AND resource = ?",
                (month, resource),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(f"{month} 资源 {resource} 尚无任何报送，没有合并摘要")
        return {
            "month": month,
            "resource": resource,
            "registers": json.loads(row["registers"]),
            "updated_at": row["updated_at"],
        }

    def get_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """读取某馆的现行报送（供换报前确认，可选辅助接口）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect(for_write=False)
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM branch_reports "
                "WHERE month = ? AND resource = ? AND branch = ?",
                (month, resource, branch),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(
                f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送"
            )
        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": json.loads(row["registers"]),
            "updated_at": row["updated_at"],
        }
