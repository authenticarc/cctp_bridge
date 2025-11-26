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

import logging

# -----------------------------
# Config
# -----------------------------
DEFAULT_DUNE_API_KEY = "pRfwvQ6unzQYU8k2XH2ixn9b8IvLKB0B"
DEFAULT_DUNE_QUERY_ID = 6261537

DUNE_API_KEY = os.getenv("DUNE_API_KEY", DEFAULT_DUNE_API_KEY)
DUNE_QUERY_ID = int(os.getenv("DUNE_QUERY_ID", str(DEFAULT_DUNE_QUERY_ID)))
DUNE_HASH_COLUMN = os.getenv("DUNE_HASH_COLUMN", "tx_hash")

DUNE_NAMESPACE = "bitgetwallet"
DUNE_TABLE_NAME = "cctp_tx_map"
DUNE_TABLE_FULL = f"{DUNE_NAMESPACE}.{DUNE_TABLE_NAME}"

CSV_PATH = "data/cctp_tx_mapping.csv"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "6.0"))

DUNE = DuneClient(DUNE_API_KEY)

ERROR_LOG_PATH = "logs/cctp_error.log"
os.makedirs(os.path.dirname(ERROR_LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    handlers=[
        logging.FileHandler(ERROR_LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# -----------------------------
# Step 1: load tx_hash from Dune
# -----------------------------
def load_dune_hashes():
    query = QueryBase(
        name="CCTP Hash Fetcher",
        query_id=DUNE_QUERY_ID,
    )
    df = DUNE.run_query_dataframe(query, performance="medium")

    if DUNE_HASH_COLUMN not in df.columns:
        raise KeyError(f"Dune result is missing column {DUNE_HASH_COLUMN}, actual columns: {df.columns}")

    df = df[[DUNE_HASH_COLUMN]].dropna().drop_duplicates()
    df[DUNE_HASH_COLUMN] = df[DUNE_HASH_COLUMN].astype(str).str.strip()
    df = df[df[DUNE_HASH_COLUMN] != ""]
    return df


class FetchError(Exception):
    pass


# -----------------------------
# Step 3: fetch CCTP info for a single tx
# -----------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(FetchError),
)
def fetch_cctp_pair(tx_hash: str) -> dict | None:
    """
    Input: one transaction hash (string)
    Output: dict with sender_tx_hash / receiver_tx_hash and related fields, or None
    """

    tx_headers = {
        "authority": "usdc.range.org",
        "method": "GET",
        "path": "/transactions?s={}".format(tx_hash),
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "next-router-state-tree": "%5B%22%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22subExplorerConfigKey%5C%22%3A%5C%22usdc%5C%22%7D%22%2C%7B%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D",
        "next-url": "/",
        "priority": "u=1, i",
        "rsc": "1",
        "sec-ch-ua": "\"Chromium\";v=\"142\", \"Microsoft Edge\";v=\"142\", \"Not_A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
    }

    # If you want to enable proxy, uncomment this and set proxies accordingly
    # proxies = {
    #     "http": 'http://user:pass@host:port',
    #     "https": 'http://user:pass@host:port',
    # }
    proxies = None

    # -------- Step 1: call /transactions -------
    url1 = f"https://usdc.range.org/transactions?s={tx_hash}"
    try:
        r1 = requests.get(url1, timeout=HTTP_TIMEOUT, proxies=proxies, headers=tx_headers)
    except Exception as e:
        logging.error("tx=%s /transactions request exception: %s", tx_hash, repr(e))
        raise FetchError(f"/transactions exception: {e}")

    if r1.status_code != 200:
        logging.error(
            "tx=%s /transactions HTTP %s, body_prefix=%s",
            tx_hash, r1.status_code, r1.text[:300],
        )
        raise FetchError(f"/transactions HTTP {r1.status_code}")

    text = r1.text
    m = re.search(r'id=([A-Za-z0-9_-]+);', text)
    if not m:
        logging.warning("tx=%s no job_id found in /transactions response", tx_hash)
        logging.warning(text)
        return None

    job_id = m.group(1)

    status_headers = {
        "authority": "usdc.range.org",
        "method": "GET",
        "path": f"/api/status?id={job_id}",
        "scheme": "https",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "priority": "u=1, i",
        "referer": f"https://usdc.range.org/status?id={job_id}",
        "sec-ch-ua": "\"Chromium\";v=\"142\", \"Microsoft Edge\";v=\"142\", \"Not_A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
    }

    # -------- Step 2: call /api/status -------
    url2 = f"https://usdc.range.org/api/status?id={job_id}"
    try:
        r2 = requests.get(url2, timeout=HTTP_TIMEOUT, proxies=proxies, headers=status_headers)
    except Exception as e:
        logging.error("tx=%s job_id=%s /api/status request exception: %s", tx_hash, job_id, repr(e))
        raise FetchError(f"/api/status exception: {e}")

    if r2.status_code != 200:
        logging.error(
            "tx=%s job_id=%s /api/status HTTP %s, body_prefix=%s",
            tx_hash, job_id, r2.status_code, r2.text[:300],
        )
        raise FetchError(f"/api/status HTTP {r2.status_code}")

    try:
        data = r2.json()
    except Exception as e:
        logging.error(
            "tx=%s job_id=%s /api/status JSON decode error: %s, text_prefix=%s",
            tx_hash, job_id, repr(e), r2.text[:300],
        )
        raise FetchError("invalid json")

    payment = data.get("payment", {})

    if not isinstance(payment, dict):
        logging.warning(
            "tx=%s job_id=%s /api/status missing 'payment' field, data_keys=%s",
            tx_hash, job_id, list(data.keys()) if isinstance(data, dict) else type(data),
        )
        return None

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
# Step 4: batch fetch with concurrency
# -----------------------------
def build_cctp_df(df_hash: pd.DataFrame) -> pd.DataFrame:
    hashes = df_hash[DUNE_HASH_COLUMN].tolist()
    total = len(hashes)

    print(f"[CCTP] total tasks={total}, workers={MAX_WORKERS}")
    logging.info("[CCTP] start, total=%d, workers=%d", total, MAX_WORKERS)

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
                # After all tenacity retries, failures end up here
                logging.exception("tx=%s final failure: %s", tx, repr(e))
                rec = None
            results.append(rec)
            done += 1

            if done % max(5, total // 20 or 1) == 0:
                elapsed = time.time() - t0
                logging.info("[CCTP] progress %d/%d (%.1f%%), elapsed=%.1fs",
                             done, total, done / total * 100, elapsed)
                print(f"[CCTP] {done}/{total} ({done/total:4.1%}) ...")

    # filter out None
    rows = [r for r in results if isinstance(r, dict)]
    if not rows:
        logging.warning("[CCTP] all tasks failed or returned no result")
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
    logging.info("[CCTP] success %d/%d, elapsed %.2fs", len(df), total, time.time() - t0)
    print(f"[CCTP] success {len(df)}/{total}, elapsed {time.time()-t0:.2f}s")
    return df


# -----------------------------
# Step 5: create / update Dune table
# -----------------------------
def ensure_table():
    try:
        DUNE.delete_table(DUNE_NAMESPACE, DUNE_TABLE_NAME)
    except Exception:
        # ignore if table does not exist
        pass

    schema = [
        {"name": "query_tx_hash", "type": "varchar"},
        {"name": "sender_tx_hash", "type": "varchar"},
        {"name": "receiver_tx_hash", "type": "varchar"},
        {"name": "sender_address", "type": "varchar"},
        {"name": "receiver_address", "type": "varchar"},
        {"name": "sender_chain", "type": "varchar"},
        {"name": "receiver_chain", "type": "varchar"},
        {"name": "bridge_type", "type": "varchar"},
        {"name": "status", "type": "varchar"},
        {"name": "job_id", "type": "varchar"},
    ]

    try:
        DUNE.create_table(
            namespace=DUNE_NAMESPACE,
            table_name=DUNE_TABLE_NAME,
            description="CCTP sender/receiver tx hash mapping",
            is_private=False,
            schema=schema,
        )
    except Exception:
        # ignore if already exists with same schema
        pass


def insert_csv(csv_path: str):
    with open(csv_path, "rb") as f:
        DUNE.insert_table(DUNE_NAMESPACE, DUNE_TABLE_NAME, f, content_type="text/csv")


# -----------------------------
# Step 6: Main
# -----------------------------
def main():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    print("Step 1) Load tx_hash from Dune")
    df_hash = load_dune_hashes()

    print("Step 2) Fetch CCTP tx mapping via API")
    df_out = build_cctp_df(df_hash)

    print("Step 3) Write CSV")
    df_out.to_csv(CSV_PATH, index=False)
    print(f"CSV written to {CSV_PATH}")

    print("Step 4) Ensure Dune table exists")
    # ensure_table()

    print("Step 5) Upload CSV to Dune table")
    insert_csv(CSV_PATH)

    print("All done")


if __name__ == "__main__":
    main()
