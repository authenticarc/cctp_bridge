from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

transactions = "0x3ec34705832931dacf8432d2264beac706a5f013386a3d959c5352cdd8228c66"

tx_url = f"https://usdc.range.org/transactions?s={transactions}"

stealth = Stealth()

with stealth.use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(
        headless=True,  # 本地调试先关掉无头
        proxy={
                "server": "http://b2bcc08abc0815ee.qzc.na.ipidea.online:2336",
                "username": "clyderen-zone-custom",
                "password": "123456",
            }
    )
    page = browser.new_page()

    try:
        page.goto(tx_url, wait_until="networkidle")

        # ✅ 这里一定要用 timeout=10000，而不是第二个位置参数 10000
        sender = page.wait_for_selector(
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
            "> div > div > div:nth-child(2)",
            timeout=10000,
        )

        receiver = page.wait_for_selector(
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
            "> div > div > div:nth-child(2)",
            timeout=10000,
        )

        print("Sender Address:", sender.inner_text())
        print("Receiver Address:", receiver.inner_text())

    finally:
        browser.close()
