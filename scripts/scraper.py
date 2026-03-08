"""
myauto.ge scraper
Uses curl_cffi to impersonate Chrome (bypasses Cloudflare TLS fingerprinting).
Uses asyncio for concurrent requests.

Requirements:
    pip install curl_cffi
"""
import asyncio
import csv
import json
import sys
from pathlib import Path

from curl_cffi.requests import AsyncSession

BASE_URL = "https://api2.myauto.ge/ka/products"
FIXED_PARAMS = {
    "TypeID": "0",
    "Mans": "",
    "CurrencyID": "3",
    "MileageType": "1",
}
HEADERS = {
    "accept": "*/*",
    "accept-language": "ka",
    "authtoken": "undefined",
    "content-type": "application/json",
    "dnt": "1",
    "origin": "https://myauto.ge",
    "referer": "https://myauto.ge/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}
IMPERSONATE = "chrome110"
CONCURRENCY = 20          # parallel requests
RETRY_LIMIT = 3           # retries per page on failure
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "data.csv"


async def fetch_page(session: AsyncSession, page: int, sem: asyncio.Semaphore) -> tuple[int, dict | None]:
    params = {**FIXED_PARAMS, "Page": str(page)}
    async with sem:
        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                r = await session.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
                if r.status_code == 200:
                    return page, r.json()
                print(f"  [page {page}] HTTP {r.status_code} (attempt {attempt})", flush=True)
            except Exception as e:
                print(f"  [page {page}] error: {e} (attempt {attempt})", flush=True)
            if attempt < RETRY_LIMIT:
                await asyncio.sleep(1)
    return page, None


def flatten(item: dict) -> dict:
    flat = {}
    for k, v in item.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                flat[f"{k}.{sub_k}"] = sub_v
        elif isinstance(v, list):
            flat[k] = json.dumps(v, ensure_ascii=False)
        else:
            flat[k] = v
    return flat


async def scrape():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(CONCURRENCY)

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        # ── page 1: discover total pages ─────────────────────────────────
        print("Fetching page 1...", flush=True)
        _, first = await fetch_page(session, 1, asyncio.Semaphore(1))
        if first is None:
            print("Failed to fetch page 1. Aborting.")
            sys.exit(1)

        data_block = first.get("data", {})
        meta = data_block.get("meta", {})
        total_pages = meta.get("last_page", 1)
        total_items = meta.get("total", "?")
        print(f"Total: {total_items} items across {total_pages} pages", flush=True)

        first_items = data_block.get("items", [])

        # ── remaining pages ───────────────────────────────────────────────
        tasks = [fetch_page(session, p, sem) for p in range(2, total_pages + 1)]

        all_items = list(first_items)
        done = 1

        for coro in asyncio.as_completed(tasks):
            page, data = await coro
            done += 1
            if data is None:
                print(f"  [page {page}] skipped (all retries failed)", flush=True)
                continue
            items = data.get("data", {}).get("items", [])
            all_items.extend(items)
            if done % 100 == 0 or done == total_pages:
                print(f"  Progress: {done}/{total_pages} pages | {len(all_items)} items", flush=True)

    # ── write CSV ─────────────────────────────────────────────────────────
    print(f"\nFlattening and writing {len(all_items)} records...", flush=True)
    flat_rows = [flatten(item) for item in all_items]

    # Preserve column order from first row, then add any extras seen later
    fieldnames = list(dict.fromkeys(k for row in flat_rows for k in row))

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat_rows)

    print(f"Done. {len(flat_rows)} records saved to {OUTPUT_PATH}", flush=True)


if __name__ == "__main__":
    asyncio.run(scrape())
