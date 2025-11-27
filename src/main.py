# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
from threading import Thread, Lock
from queue import Queue

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from dune_client.client import DuneClient
from dune_client.query import QueryBase
from playwright.sync_api import sync_playwright, Page
from playwright_stealth import Stealth

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

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15.0"))  # 页面加载超时（秒）
N_BROWSERS = int(os.getenv("N_BROWSERS", "5"))           # 浏览器池大小（并发度）

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
def load_dune_hashes() -> pd.DataFrame:
    query = QueryBase(
        name="CCTP Hash Fetcher",
        query_id=DUNE_QUERY_ID,
    )
    df = DUNE.run_query_dataframe(query, performance="medium")

    if DUNE_HASH_COLUMN not in df.columns:
        raise KeyError(
            f"Dune result is missing column {DUNE_HASH_COLUMN}, actual columns: {df.columns}"
        )

    df = df[[DUNE_HASH_COLUMN]].dropna().drop_duplicates()
    df[DUNE_HASH_COLUMN] = df[DUNE_HASH_COLUMN].astype(str).str.strip()
    df = df[df[DUNE_HASH_COLUMN] != ""]
    logging.info("Loaded %d unique tx_hash from Dune", len(df))
    return df


class FetchError(Exception):
    """For tenacity retries."""
    pass


# -----------------------------
# Step 2: selector（你给的那两个）
# -----------------------------
SENDER_SELECTOR = (
    "body > div > div.mx-4.md\\:mx-20.flex.flex-col.items-center.justify-center.gap-4 "
    "> div.flex.flex-col.gap-5xl.w-full.mt-14 "
    "> div.whitespace-nowrap "
    "> div.flex.flex-col.gap-11.pt-3xl "
    "> div.flex.flex-col.gap-5xl.w-full "
    "> div.flex.flex-col.gap-9 "
    "> div:nth-child(1) "
    "> div.flex.flex-col.gap-1.w-full "
    "> div.flex.flex-col.sm\\:flex-row.gap-md.sm\\:items-center.w-full "
    "> div.flex.w-full.sm\\:w-\\[calc\\(100\\%-250px\\)\\].items-center "
    "> div > div > div:nth-child(2)"
)

RECEIVER_SELECTOR = (
    "body > div > div.mx-4.md\\:mx-20.flex.flex-col.items-center.justify-center.gap-4 "
    "> div.flex.flex-col.gap-5xl.w-full.mt-14 "
    "> div.whitespace-nowrap "
    "> div.flex.flex-col.gap-11.pt-3xl "
    "> div.flex.flex-col.gap-5xl.w-full "
    "> div.flex.flex-col.gap-9 "
    "> div:nth-child(2) "
    "> div.flex.flex-col.gap-1.w-full "
    "> div.flex.flex-col.sm\\:flex-row.gap-md.sm\\:items-center.w-full "
    "> div.flex.w-full.sm\\:w-\\[calc\\(100\\%-250px\\)\\].items-center "
    "> div > div > div:nth-child(2)"
)


# -----------------------------
# Step 3: 在「已有 page」上抓一笔（有重试）
# -----------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(FetchError),
)
def fetch_sender_receiver_on_page(tx_hash: str, page: Page) -> dict | None:
    """
    输入：一个 tx_hash 和该线程持有的 page
    输出：包含 query_tx_hash / sender_address / receiver_address 的 dict
    """
    tx_url = f"https://usdc.range.org/transactions?s={tx_hash}"
    logging.info("Fetching tx=%s url=%s", tx_hash, tx_url)

    try:
        page.goto(tx_url, wait_until="networkidle", timeout=HTTP_TIMEOUT * 1000)

        sender_el = page.wait_for_selector(SENDER_SELECTOR, timeout=10000)
        receiver_el = page.wait_for_selector(RECEIVER_SELECTOR, timeout=10000)

        sender_txt = sender_el.inner_text().strip()
        receiver_txt = receiver_el.inner_text().strip()

        return {
            "query_tx_hash": tx_hash.lower(),
            "sender_address": sender_txt,
            "receiver_address": receiver_txt,
        }

    except Exception as e:
        logging.warning("tx=%s fetch error (will retry if attempts left): %s", tx_hash, repr(e))
        # 抛 FetchError 触发 tenacity 重试
        raise FetchError(f"fetch_sender_receiver failed for tx={tx_hash}: {e}") from e


# -----------------------------
# Step 4: 浏览器工作线程（每个线程一个 browser + page）
# -----------------------------
def browser_worker(name: str, task_queue: Queue, results: list, lock: Lock):
    """
    每个 worker 线程：
      - 初始化自己的 Playwright + Browser + Page（挂 rotating proxy）
      - 不断从队列中取 tx_hash，顺序处理
      - 处理完所有任务后退出
    """
    logging.info("%s starting", name)
    stealth = Stealth()
    with stealth.use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(
            headless=True,
            proxy={
                "server": "http://b2bcc08abc0815ee.qzc.na.ipidea.online:2336",
                "username": "clyderen-zone-custom",
                "password": "123456",
            },
        )
        page = browser.new_page()

        try:
            while True:
                tx = task_queue.get()
                if tx is None:
                    # 取到哨兵值，说明没有任务了
                    task_queue.task_done()
                    logging.info("%s got sentinel, exiting", name)
                    break

                try:
                    rec = fetch_sender_receiver_on_page(tx, page)
                except Exception as e:
                    logging.exception("%s tx=%s final failure after retries: %s", name, tx, repr(e))
                    rec = None

                if isinstance(rec, dict):
                    with lock:
                        results.append(rec)

                task_queue.task_done()
        finally:
            browser.close()
            logging.info("%s browser closed", name)


def build_cctp_df(df_hash: pd.DataFrame) -> pd.DataFrame:
    hashes = df_hash[DUNE_HASH_COLUMN].tolist()
    total = len(hashes)

    logging.info(
        "[CCTP] start, total=%d, browsers=%d (browser pool with rotating proxy)",
        total,
        N_BROWSERS,
    )
    print(f"[CCTP] total tasks={total}, browsers={N_BROWSERS}")

    task_queue: Queue = Queue()
    results: list[dict] = []
    lock = Lock()

    # 把任务塞进队列
    for h in hashes:
        task_queue.put(h)

    # 加 N_BROWSERS 个哨兵 None，表示任务结束
    for _ in range(N_BROWSERS):
        task_queue.put(None)

    # 启动浏览器线程
    threads: list[Thread] = []
    t0 = time.time()

    for i in range(N_BROWSERS):
        t = Thread(
            target=browser_worker,
            name=f"browser-worker-{i+1}",
            args=(f"browser-worker-{i+1}", task_queue, results, lock),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # 阻塞直到所有任务完成
    task_queue.join()

    elapsed = time.time() - t0
    logging.info("[CCTP] all tasks done, elapsed=%.2fs, results=%d", elapsed, len(results))
    print(f"[CCTP] all tasks done, elapsed={elapsed:.2f}s, results={len(results)}")

    if not results:
        logging.warning("[CCTP] all tasks failed or returned no result")
        return pd.DataFrame(columns=["query_tx_hash", "sender_address", "receiver_address"])

    df = pd.DataFrame(results).drop_duplicates()
    return df


# -----------------------------
# Step 5: Dune table 逻辑（三列版本）
# -----------------------------
def ensure_table():
    """
    如果你之后想重新建表，可以打开 main() 里 ensure_table() 调用。
    这里只保留三列：query_tx_hash, sender_address, receiver_address。
    """
    try:
        DUNE.delete_table(DUNE_NAMESPACE, DUNE_TABLE_NAME)
    except Exception:
        # ignore if table does not exist
        pass

    schema = [
        {"name": "query_tx_hash", "type": "varchar"},
        {"name": "sender_address", "type": "varchar"},
        {"name": "receiver_address", "type": "varchar"},
    ]

    try:
        DUNE.create_table(
            namespace=DUNE_NAMESPACE,
            table_name=DUNE_TABLE_NAME,
            description="CCTP sender/receiver address mapping",
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

    print("Step 2) Fetch CCTP sender/receiver via browser pool")
    df_out = build_cctp_df(df_hash)

    print("Step 3) Write CSV")
    df_out.to_csv(CSV_PATH, index=False)
    print(f"CSV written to {CSV_PATH}")

    print("Step 4) Ensure Dune table exists (optional)")
    # ensure_table()

    print("Step 5) Upload CSV to Dune table")
    insert_csv(CSV_PATH)

    print("All done")


if __name__ == "__main__":
    main()
