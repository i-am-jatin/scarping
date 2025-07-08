# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 15:20:13 2025
Last Modified on Tue Jul 08 17:49:52 2025
@author: Jatin Bhardwaj
"""

# Libraries
import asyncio
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import unquote
import pandas as pd
import re
from datetime import datetime
from playwright.async_api import async_playwright
import threading
from pandas.api.types import CategoricalDtype

# ================= LOGGING ====================
class ThreadLogger:
    def __init__(self):
        self.logs = []
        self.context = None
        self.lock = threading.Lock()
        self.counter = 0

    def set_context(self, context):
        self.context = context
        self.counter = 0

    def log(self, message, indent=0, emoji="➡️"):
        self.counter += 1
        prefix = f"{'    ' * indent}{emoji} [{self.context}.{self.counter}]"
        with self.lock:
            self.logs.append(f"{prefix} {message}")

    def get_logs(self):
        return self.logs.copy()

# ========== CONFIG FILE ==========
config_path = Path.cwd() / "config/make_model.csv"
config = pd.read_csv(config_path, usecols=["make", "year", "invygo_model"], low_memory=False)
config = config[config["invygo_model"].notna() & (config["invygo_model"].str.strip() != "")]
filename = Path.cwd() / f"output/invygo_rentals.xlsx"

async def make_fast_firefox_async():
    playwright = await async_playwright().start()
    browser = await playwright.firefox.launch(
        headless=True,
        args=["-private"],
        firefox_user_prefs={
            "permissions.default.image": 2,
            "dom.ipc.processCount": 1,
            "browser.tabs.remote.autostart": False,
            "network.dns.disablePrefetch": True,              
            "network.http.use-cache": False,
            "toolkit.cosmeticAnimations.enabled": False,
            "layout.css.animation.enabled": False,
            "layout.css.transition.enabled": False,    
            "general.smoothScroll": False,
            "ui.prefersReducedMotion": 1,                    
        }
    )
    context = await browser.new_context()
    return browser, context

# ========== DATA CLEANING HELPERS ==========
def extract_make_model_from_url(url):
    url = unquote(url)
    match = re.search(r'rent-(?:weekly|monthly)-([a-z0-9\- ]+)-\d{4}', url)
    if match:
        parts = match.group(1).strip().split('-')
        if len(parts) >= 2:
            make = parts[0]
            model = '-'.join(parts[1:])
            return make, model
    return None, None

def clean_price(text):
    if not text:
        return None
    cleaned = (
        text.replace('\xa0', ' ')
            .replace('AED', '')
            .replace('Save', '')
            .replace('/ mo', '')
            .replace('/ day', '') 
            .replace('months', '')
            .replace('month', '')                       
            .replace(',', '')
            .strip()
    )
    return cleaned

def extract_numeric(text):
    if pd.isnull(text):
        return 0
    if "No additional cost" in text:
        return 0
    nums = ''.join(filter(str.isdigit, text))
    return int(nums) if nums else 0

async def scroll_to_bottom(page, pause=2, max_attempts=3):
    last_height = 0
    for _ in range(max_attempts):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Define custom sort orders
contract_order = CategoricalDtype(["weekly", "monthly"], ordered=True)
duration_order = CategoricalDtype(["1 week", "1 month", "3 months", "6 months", "9 months"], ordered=True)

# ========== MAIN PAGE SCRAPER ==========
async def scrape_invigo_car_data(page, url, mode):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")
    await page.wait_for_selector("a[href^='/en-ae/dubai/rent-'] div.p-4", timeout=10000)
    await scroll_to_bottom(page)
    await asyncio.sleep(5)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    car_cards = soup.find_all("a", href=True)

    extracted = []
    for card in car_cards:
        try:
            href_match = f"/en-ae/dubai/rent-{mode}-"
            if not card["href"].startswith(href_match):
                continue

            info_div = card.find("div", class_="p-4 space-y-2")
            if not info_div:
                continue

            year_tag = info_div.find("p", class_="text-[#667085] text-xs font-medium")
            year = int(year_tag.text.strip()) if year_tag else None

            title_tag = info_div.find("h3", class_="text-[#0C111D] font-semibold text-sm")
            title = title_tag.text.strip() if title_tag else None

            contract_tag = info_div.find_all("div", class_="text-[#0C111D] font-semibold text-xs")
            mileage = contract_tag[1].text.strip() if len(contract_tag) > 1 else None

            promo_tag = card.find("div", class_=re.compile(r"bg-\[#?EC625B\]"))
            promotion = "yes" if promo_tag else ""

            full_url = f"https://invygo.com{card['href']}"

            extracted.append({
                "sub-url": full_url,
                "title": title,
                "year": year,
                "runnings_kms": mileage,
                "promotion": promotion,
                "contract": mode
            })

        except Exception as e:
            print("❌ Error parsing card:", e)
            continue

    return pd.DataFrame(extracted)

# ========== DETAIL PAGE SCRAPER ==========
async def extract_subscription_details(page, url):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")

    try:
        await page.wait_for_selector('[data-testid="booking-contract-length"] [role="presentation"]', timeout=10000)
    except:
        print(f"❌ Timeout: Contract durations not loaded at {url}")
        return []

    await scroll_to_bottom(page)
    await asyncio.sleep(3)

    enriched_data = []
    seen_durations = set()

    duration_count = len(await page.query_selector_all('[data-testid="booking-contract-length"] [role="presentation"]'))

    for index in range(duration_count):
        try:
            # Refresh element handles in case DOM changed
            duration_elements = await page.query_selector_all('[data-testid="booking-contract-length"] [role="presentation"]')
            elem = duration_elements[index]

            await elem.scroll_into_view_if_needed()
            await asyncio.sleep(0.5)
            await elem.click()
            await asyncio.sleep(2)

            # Wait for price to ensure DOM has updated
            await page.wait_for_selector(".text-black.font-inter.text-3xl.font-bold.leading-8", timeout=5000)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            duration_block = soup.select('[data-testid="booking-contract-length"] [role="presentation"]')[index]
            duration_text = duration_block.select_one('div.text-cool-gray-900').text.strip()

            # Skip if already seen (some sites reuse same blocks)
            if duration_text in seen_durations:
                continue
            seen_durations.add(duration_text)

            savings_tag = duration_block.select_one('div.text-grey-50')
            savings_text = savings_tag.text.strip() if savings_tag else "AED 0"
            savings_value = int(clean_price(savings_text))

            price_tag = soup.find("div", class_="text-black font-inter text-3xl font-bold leading-8")
            price = int(clean_price(price_tag.text.strip())) if price_tag else None

            insurance_result = {"standard_cover_insurance": None, "full_cover_insurance": None}
            insurance_blocks = soup.select('[data-testid="booking-insurance-options"] [role="presentation"]')
            for block in insurance_blocks:
                title = block.select_one('div.text-cool-gray-900')
                note = block.select_one('div.text-grey-50')
                if title:
                    title_text = title.text.strip().lower()
                    note_text = note.text.strip() if note else None
                    if "standard cover" in title_text:
                        insurance_result["standard_cover_insurance"] = note_text
                    elif "full cover" in title_text:
                        insurance_result["full_cover_insurance"] = note_text

            mileage_blocks = soup.select('[data-testid="booking-milage-options"] [role="presentation"]')
            mileage_list = []
            for m in mileage_blocks:
                m_text = m.select_one('div.text-cool-gray-900')
                m_note = m.select_one('div.text-grey-50')
                if m_text:
                    mileage_list.append({
                        "mileage": m_text.text.strip(),
                        "mileage_note": m_note.text.strip() if m_note else None,
                        "mileage_numeric": extract_numeric(m_note.text.strip()) if m_note else 0
                    })

            for mileage_entry in mileage_list:
                enriched_data.append({
                    "sub-url": url,
                    "duration": duration_text,
                    "savings": savings_value,
                    "offered_price": price,
                    **mileage_entry,
                    **insurance_result
                })

        except Exception as e:
            print(f"⚠️ Failed to extract for duration #{index} at {url}: {e}")
            continue

    return enriched_data

# ========== PARALLEL DETAIL SCRAPER ==========

async def scrape_detail_with_new_page(browser, url, logger, sem):
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()
        try:
            logger.log(f"Scraping: {url}", indent=1)
            return await extract_subscription_details(page, url)
        except Exception as e:
            logger.log(f"❌ Failed scraping {url}: {e}", indent=1)
            return []
        finally:
            await context.close()

# ========== MAIN RUNNER ==========
async def main():
    print("🚀 Launching async scraping for weekly and monthly...")
    modes = ["weekly", "monthly"]
    all_main_dfs, all_detail_dicts, all_logs = [], [], []

    browser, _ = await make_fast_firefox_async()
    try:
        for mode in modes:
            logger = ThreadLogger()
            context = await browser.new_context()
            page = await context.new_page()

            try:
                url = f"https://invygo.com/en-ae/dubai/rent-{mode}-cars"
                df = await scrape_invigo_car_data(page, url, mode)
                logger.set_context(mode.upper())
                logger.log(f"Loaded {len(df)} listings for {mode}")

                df[["make", "model"]] = df["sub-url"].apply(lambda url: pd.Series(extract_make_model_from_url(url)))

                df = df.dropna(subset=["make", "model", "year"])
                df[["make", "model", "year"]] = df[["make", "model", "year"]].astype(str).apply(lambda col: col.str.upper())

                config_renamed = config.rename(columns={"invygo_model": "model"})
                config_renamed = config_renamed.astype(str).apply(lambda col: col.str.upper())

                df_filtered = df.merge(config_renamed, on=["make", "model", "year"], how="inner")
                df_filtered["year"] = pd.to_numeric(df_filtered["year"], errors="coerce").astype("Int64")
                df_filtered['title'] = df_filtered['title'].str.lower() + ' ' + df_filtered['year'].astype(str)
                
                if df_filtered.empty:
                    logger.log(f"⚠️ No {mode} results matched config.")
                    all_logs.extend(logger.get_logs())
                    continue 

                sem = asyncio.Semaphore(5)
                sub_urls = df_filtered["sub-url"].tolist()
                tasks = [scrape_detail_with_new_page(browser, url, logger, sem) for url in sub_urls]
                
                try:
                    results = await asyncio.gather(*tasks)
                    details = [item for sublist in results for item in sublist]
                    logger.log(f"✅ Completed detail scraping: {len(details)} rows collected.")
                except Exception as e:
                    logger.log(f"❌ Error during detail scraping: {e}")                

                all_main_dfs.append(df_filtered)
                all_detail_dicts.extend(details)
                all_logs.extend(logger.get_logs())
            finally:
                await context.close()
    finally:
        await browser.close()

    main_df = pd.concat(all_main_dfs, ignore_index=True)
    detail_df = pd.DataFrame(all_detail_dicts)

    if not detail_df.empty:
        duration_num = detail_df["duration"].str.extract(r"(\d+)")[0].astype("Int64")
        detail_df["base_price"] = (detail_df["savings"] / duration_num) + detail_df["offered_price"]
        if 'mileage_numeric' in detail_df.columns:
            mileage = detail_df.pop("mileage_numeric")
            detail_df["base_price"] += mileage
            detail_df["offered_price"] += mileage

    final_df = pd.merge(main_df, detail_df, on="sub-url", how="left")
    final_df["contract"] = final_df["contract"].astype(contract_order)
    final_df["duration"] = final_df["duration"].astype(duration_order)


    df_sorted = final_df.sort_values(by=["contract", "sub-url", "duration", "mileage"]).reset_index(drop=True)

    invygo_df = df_sorted[[
        "sub-url", "title", "make", "model", "year", "promotion", "runnings_kms",
        "contract", "base_price", "savings", "offered_price", "duration", "mileage",
        "mileage_note", "standard_cover_insurance", "full_cover_insurance"]]

    invygo_df.to_excel(filename, index=False)
    print(f"\n📁 Saved: {filename}")
    print("\n✅ All scraping complete.")

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())