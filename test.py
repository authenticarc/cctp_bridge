import requests
import re

transactions = '0xe2c4132a498e1dc20323ffdcaa5c35ee9ce7f6b5d52eb2e78bcaa7dd7eafc766'

# ====== 统一会话 & Cookies ======
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
})

cookies = {
    "_ga": "GA1.1.904511668.1764137977",
    "cf_clearance": "p9RlA05Vq_J1A5Hol6Wl4hQEC5qzvLFOr.S3IBgV2hc-1764137981-1.2.1.1-kbprLeBJ.M50JQfWU5l3yGKL7auuYbT19p7AOYMR0G4_2mBcYlR_5CtOIeL.QE2lXM6660P6bruqAjzHkZfn3WF2PLSUErZaVs8KJ1SALvkmxkuIuGVsdZthGnOQNl2bJxw0e8HapW6ZEneXlP5qtl7rHAOTRas7pHteSereSqW0N7Uca0LusvhszLyYElEuB4SjwmmWlK1ormUW2CZ8bg.Sh1MMGEok6Jfo5Qu5kNw",
    "ph_phc_zQDlkkBzhz2IiuDM3AQYHV9KOO72eEe0JyLNifbbHZh_posthog": "%7B%22distinct_id%22%3A%22019abed1-42f7-7ccb-9823-ce5c96b2e362%22%2C%22%24sesid%22%3A%5B1764138550945%2C%22019abed1-42f6-7fd9-ab02-5e5da3d7d376%22%2C1764137976566%5D%7D",
    "_ga_FEZ2WGXW9F": "GS2.1.s1764137976$o1$g1$t1764138551$j58$l0$h1705058839",
}
for k, v in cookies.items():
    session.cookies.set(k, v, domain=".range.org", path="/")

# ====== 第一步：访问 /transactions 拿 id ======
tx_url = "https://usdc.range.org/transactions?s={}".format(transactions)

tx_headers = {
    "authority": "usdc.range.org",
    "method": "GET",
    "path": "/transactions?s={}".format(transactions),
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
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
}

tx_resp = session.get(tx_url, headers=tx_headers)
tx_text = tx_resp.text

# 从返回中提取 id（就是那个长串 base64url）
m = re.search(r'id=([A-Za-z0-9_-]+);', tx_text)
if not m:
    raise RuntimeError("没有在 /transactions 响应中找到 id")
job_id = m.group(1)
print("job_id =", job_id)

# ====== 第二步：用 id 调用 /api/status?id=... ======
status_url = f"https://usdc.range.org/api/status?id={job_id}"

status_headers = {
    "authority": "usdc.range.org",
    "method": "GET",
    "path": f"/api/status?id={job_id}",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "priority": "u=1, i",
    "referer": f"https://usdc.range.org/status?id={job_id}",
    "sec-ch-ua": "\"Chromium\";v=\"142\", \"Microsoft Edge\";v=\"142\", \"Not_A Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

status_resp = session.get(status_url, headers=status_headers)

print(status_resp.status_code)
# 如果返回的是 JSON（大概率是），直接解析：
try:
    print(status_resp.json())
except ValueError:
    print(status_resp.text)

import pandas as pd

data = status_resp.json()
payment = data["payment"]

sender_tx_hash = payment.get("sender_tx_hash")
receiver_tx_hash = payment.get("receiver_tx_hash")

df = pd.DataFrame([{
    "sender_tx_hash": sender_tx_hash,
    "receiver_tx_hash": receiver_tx_hash,
}])

print(df)
