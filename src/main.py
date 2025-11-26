# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from dune_client.client import DuneClient
from dune_client.query import QueryBase

# -----------------------------
# 配置区（根据你当前脚本结构复制）
# -----------------------------
DEFAULT_DUNE_API_KEY = "pRfwvQ6unzQYU8k2XH2ixn9b8IvLKB0B"
DEFAULT_DUNE_QUERY_ID = 6261537

DUNE_API_KEY = os.getenv("DUNE_API_KEY", DEFAULT_DUNE_API_KEY)
DUNE_QUERY_ID = int(os.getenv("DUNE_QUERY_ID", str(DEFAULT_DUNE_QUERY_ID)))
DUNE_HASH_COLUMN = os.getenv("DUNE_HASH_COLUMN", "tx_hash")

DUNE_NAMESPACE = "bitgetwallet"
DUNE_TABLE_NAME = "cctp_tx_mapping"
DUNE_TABLE_FULL = f"{DUNE_NAMESPACE}.{DUNE_TABLE_NAME}"

CSV_PATH = "data/cctp_tx_mapping.csv"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "32"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "6.0"))

DUNE = DuneClient(DUNE_API_KEY)

# -----------------------------
# Step 1: 从 Dune 拉取 tx_hash
# -----------------------------
def load_dune_hashes():
    query = QueryBase(
        name="CCTP Hash Fetcher",
        query_id=DUNE_QUERY_ID,
    )
    df = DUNE.run_query_dataframe(query, performance="medium")

    if DUNE_HASH_COLUMN not in df.columns:
        raise KeyError(f"Dune 缺失列 {DUNE_HASH_COLUMN}, 实际 {df.columns}")

    df = df[[DUNE_HASH_COLUMN]].dropna().drop_duplicates()
    df[DUNE_HASH_COLUMN] = df[DUNE_HASH_COLUMN].astype(str).str.strip()
    df = df[df[DUNE_HASH_COLUMN] != ""]
    return df


# -----------------------------
# Step 2: CCTP API session
# -----------------------------
GLOBAL_CCTP_SESSION = requests.Session()
GLOBAL_CCTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
})
CCTP_COOKIES = {
    "_ga": "GA1.1.904511668.1764137977",
    "cf_clearance": "p9RlA05Vq_J1A5Hol6Wl4hQEC5qzvLFOr.S3IBgV2hc-1764137981-1.2.1.1-kbprLeBJ.M50JQfWU5l3yGKL7auuYbT19p7AOYMR0G4_2mBcYlR_5CtOIeL.QE2lXM6660P6bruqAjzHkZfn3WF2PLSUErZaVs8KJ1SALvkmxkuIuGVsdZthGnOQNl2bJxw0e8HapW6ZEneXlP5qtl7rHAOTRas7pHteSereSqW0N7Uca0LusvhszLyYElEuB4SjwmmWlK1ormUW2CZ8bg.Sh1MMGEok6Jfo5Qu5kNw",
    "ph_phc_zQDlkkBzhz2IiuDM3AQYHV9KOO72eEe0JyLNifbbHZh_posthog": "%7B%22distinct_id%22%3A%22019abed1-42f7-7ccb-9823-ce5c96b2e362%22%2C%22%24sesid%22%3A%5B1764138550945%2C%22019abed1-42f6-7fd9-ab02-5e5da3d7d376%22%2C1764137976566%5D%7D",
    "_ga_FEZ2WGXW9F": "GS2.1.s1764137976$o1$g1$t1764138551$j58$l0$h1705058839"
}
for k, v in CCTP_COOKIES.items():
    GLOBAL_CCTP_SESSION.cookies.set(k, v, domain=".range.org", path="/")


class FetchError(Exception):
    pass


# -----------------------------
# Step 3: 单条 CCTP 查询逻辑
# -----------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(FetchError),
)
def fetch_cctp_pair(tx_hash: str) -> dict | None:
    """
    输入：任意 transaction hash
    输出：dict(sender_tx_hash, receiver_tx_hash)
    """

    session = GLOBAL_CCTP_SESSION
    proxies = {
        "http": 'http://clyderen-zone-custom:123456@b2bcc08abc0815ee.qzc.na.ipidea.online:2336',
        "https": 'http://clyderen-zone-custom:123456@b2bcc08abc0815ee.qzc.na.ipidea.online:2336',
    }

    # -------- 第一步：访问 /transactions -------
    url1 = f"https://usdc.range.org/transactions?s={tx_hash}"
    r1 = session.get(url1, timeout=HTTP_TIMEOUT,proxies=proxies)

    if r1.status_code != 200:
        raise FetchError(f"/transactions HTTP {r1.status_code}")

    text = r1.text
    m = re.search(r'id=([A-Za-z0-9_-]+);', text)
    if not m:
        return None

    job_id = m.group(1)

    # -------- 第二步：访问 /api/status -------
    url2 = f"https://usdc.range.org/api/status?id={job_id}"
    r2 = session.get(url2, timeout=HTTP_TIMEOUT,proxies=proxies)

    if r2.status_code != 200:
        raise FetchError(f"/api/status HTTP {r2.status_code}")

    data = r2.json()
    payment = data.get("payment", {})

    return {
        "query_tx_hash": tx_hash.lower(),
        "sender_tx_hash": payment.get("sender_tx_hash"),
        "receiver_tx_hash": payment.get("receiver_tx_hash"),
        "sender_address": payment.get("sender", {}).get("address"),
        "receiver_address": payment.get("receiver", {}).get("address"),
        "sender_chain": payment.get("sender", {}).get("network"),
        "receiver_chain": payment.get("receiver", {}).get("network"),
        "bridge_type": payment.get("type"),
        "status": payment.get("status"),
        "job_id": job_id,
    }


# -----------------------------
# Step 4: 并发批量获取
# -----------------------------
def build_cctp_df(df_hash: pd.DataFrame) -> pd.DataFrame:
    hashes = df_hash[DUNE_HASH_COLUMN].tolist()
    total = len(hashes)

    print(f"[CCTP] 任务量={total}, 并发={MAX_WORKERS}")

    results = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_map = {ex.submit(fetch_cctp_pair, h): h for h in hashes}
        done = 0
        for fut in as_completed(fut_map):
            tx = fut_map[fut]
            try:
                rec = fut.result()
            except Exception as e:
                rec = None
            results.append(rec)
            done += 1

            if done % max(5, total // 20) == 0:
                print(f"[CCTP] {done}/{total} ({done/total:4.1%}) ...")

    rows = [r for r in results if r]
    if not rows:
        return pd.DataFrame(columns=[
            "query_tx_hash",
            "sender_tx_hash",
            "receiver_tx_hash",
            "sender_address",
            "receiver_address",
            "sender_chain",
            "receiver_chain",
            "bridge_type",
            "status",
            "job_id",
        ])

    df = pd.DataFrame(rows).drop_duplicates()
    print(f"[CCTP] 成功 {len(df)}/{total}，耗时 {time.time()-t0:.2f}s")
    return df


# -----------------------------
# Step 5: 写入 Dune Table
# -----------------------------
def ensure_table():
    try:
        DUNE.delete_table(DUNE_NAMESPACE, DUNE_TABLE_NAME)
    except:
        pass

    schema = [
        {"name": "query_tx_hash", "type": "varbinary"},
        {"name": "sender_tx_hash", "type": "varbinary"},
        {"name": "receiver_tx_hash", "type": "varbinary"},
        {"name": "sender_address", "type": "varbinary"},
        {"name": "receiver_address", "type": "varbinary"},
        {"name": "sender_chain", "type": "varchar"},
        {"name": "receiver_chain", "type": "varchar"},
        {"name": "bridge_type", "type": "varchar"},
        {"name": "status", "type": "varchar"},
        {"name": "job_id", "type": "varchar"},
    ]

    DUNE.create_table(
        namespace=DUNE_NAMESPACE,
        table_name=DUNE_TABLE_NAME,
        description="CCTP sender/receiver tx hash mapping",
        is_private=False,
        schema=schema,
    )


def insert_csv(csv_path: str):
    with open(csv_path, "rb") as f:
        DUNE.insert_table(DUNE_NAMESPACE, DUNE_TABLE_NAME, f, content_type="text/csv")


# -----------------------------
# Step 6: Main
# -----------------------------
def main():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    print("Step 1) 从 Dune 获取交易哈希")
    df_hash = load_dune_hashes()

    print("Step 2) 调用 CCTP API 查询 tx mapping")
    df_out = build_cctp_df(df_hash)

    print("Step 3) 写 CSV")
    df_out.to_csv(CSV_PATH, index=False)
    print(f"CSV 写入 {CSV_PATH}")

    # print("Step 4) 更新 Dune 表结构")
    # ensure_table()

    print("Step 5) 上传数据到 Dune")
    insert_csv(CSV_PATH)

    print("全部完成 ✔✔✔")


if __name__ == "__main__":
    main()
