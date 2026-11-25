#!/usr/bin/env python3
"""仅用标准库的报送示例：同一哈希口径生成 HLL-128 摘要并调用报送接口。

用法：
  python examples/submit_example.py http://localhost:8000 2026-11 ER-0001 LIB-A
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.hll import REGISTER_COUNT, build_summary  # noqa: E402

# 写锁争用时服务端返回 503 + Retry-After，客户端按提示重试同一请求即可。
MAX_RETRIES = 5


def post_with_retry(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(MAX_RETRIES + 1):
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 503 and attempt < MAX_RETRIES:
                # 可重试失败：该笔尚未入账，原样重发
                wait = float(exc.headers.get("Retry-After", "1"))
                print(f"写锁占用（503），{wait:.0f}s 后重试（{attempt + 1}/{MAX_RETRIES}）")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("超过最大重试次数仍未取得写锁")


def main(base_url: str, month: str, resource: str, branch: str) -> None:
    # 本馆本月该资源的去重读者标识；各馆务必使用 app.hll 同一哈希口径
    reader_ids = [f"{branch}-reader-{i}" for i in range(500)]
    payload = {
        "month": month,
        "resource": resource,
        "branch": branch,
        "registers": build_summary(reader_ids),
    }
    assert len(payload["registers"]) == REGISTER_COUNT

    result = post_with_retry(f"{base_url.rstrip('/')}/reports", payload)
    print("报送成功，合并后非零寄存器数：",
          sum(1 for x in result["merged"] if x))

    # 下载合并摘要
    with urllib.request.urlopen(
        f"{base_url}/merged/{month}/{resource}/download", timeout=30
    ) as resp:
        merged = json.loads(resp.read().decode("utf-8"))
    print("下载合并摘要：", json.dumps(merged, ensure_ascii=False)[:120], "...")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 4:
        print(__doc__)
        raise SystemExit(1)
    main(*args)
