# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 15:20:13 2025
Last Modified on Tue Jul 08 17:49:52 2025
@author: Jatin Bhardwaj
"""

# Libraries
from pathlib import Path
import pandas as pd
import re
import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
from pandas.api.types import CategoricalDtype
from playwright.async_api import async_playwright
import threading

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

config_path = Path.cwd() / "config/make_model.csv"
config = pd.read_csv(config_path, usecols=["make", "year", "dubizzle_model"], low_memory=False)
config = config[config["dubizzle_model"].notna() & (config["dubizzle_model"].str.strip() != "")]
filename = Path.cwd() / f"output/dubizzle_rentals.xlsx"

contract_order = CategoricalDtype(["daily", "weekly", "monthly"], ordered=True)

def extract_numeric(text):
    if not text:
        return None
    nums = ''.join(filter(str.isdigit, text))
    return int(nums) if nums else None

def fix_spacing(text):
    if pd.isna(text): return text
    text = re.sub(r"AED(\d+)", r"AED \1", text)
    text = re.sub(r"(?<=\d)(?=km)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\d)(?=for)", " ", text)  # space before "for"
    return text

async def make_fast_firefox_async():
    playwright = await async_playwright().start()
    browser = await playwright.firefox.launch(
        headless=True,
        args=["-private"],
        firefox_user_prefs={
            "permissions.default.image": 2,
            "media.autoplay.default": 0,
            "browser.shell.checkDefaultBrowser": False,
            "browser.startup.page": 0,
            "toolkit.cosmeticAnimations.enabled": False,
            "layout.css.animation.enabled": False,
            "layout.css.transition.enabled": False,
            "general.smoothScroll": False,
            "ui.prefersReducedMotion": 1,
            "network.dns.disablePrefetch": True,
            "network.prefetch-next": False,
            "network.http.use-cache": False,
            "dom.ipc.processCount": 1,
            "browser.tabs.remote.autostart": False,
        }
    )
    context = await browser.new_context()
    return browser, context

async def scroll_to_bottom_async(page, pause=2, max_attempts=3):
    for _ in range(max_attempts):
        prev_height = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            break

async def scrape_dubizzle_car_data_async(page, url):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")
    await page.wait_for_selector("#listing-card-wrapper a[data-testid^='listing-']", timeout=10000)
    await scroll_to_bottom_async(page)
    await asyncio.sleep(2)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    car_cards = soup.select("#listing-card-wrapper a[data-testid^='listing-']")
    data = []
    for card in car_cards:
        try:
            full_url = "https://dubai.dubizzle.com" + card.get("href", "")
            name_tags = card.select("h3[data-testid^='heading-text']")
            car_name = name_tags[0].text.strip() if len(name_tags) > 0 else ""
            model = name_tags[1].text.strip() if len(name_tags) > 1 else ""
            variant = name_tags[2].text.strip() if len(name_tags) > 2 else ""
            year_tag = card.select_one("h3[data-testid='listing-year']")
            year = extract_numeric(year_tag.text) if year_tag else None
            is_featured = "Yes" if card.select_one("[data-testid='featured-badge']") else ""
            data.append({
                "sub-url": full_url,
                "make": car_name,
                "model": model,
                "variant": variant,
                "year": year,
                "is_featured": is_featured
            })
        except:
            continue
    return pd.DataFrame(data)

async def scrape_dubizzle_detail_async(page, url):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")

    try:
        await page.wait_for_selector("h6[data-testid='listing-sub-heading'], h5[data-testid^='rental-price-']", timeout=10000)
    except:
        print(f"❌ Timeout on page: {url}")
        return []

    await scroll_to_bottom_async(page)
    await asyncio.sleep(2)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    enriched = []

    # ✅ Updated safe_select that supports text and attribute extraction
    def safe_select(selector, attr="text", many=False):
        try:
            el = soup.select(selector)
            if not el:
                return "" if not many else []
            if attr == "text":
                return [e.get_text(strip=True) for e in el] if many else el[0].get_text(strip=True)
            else:
                return [e.get(attr, "") for e in el] if many else el[0].get(attr, "")
        except:
            return "" if not many else []

    # ✅ Extract dealer URL properly
    dealer_url = safe_select("a[data-testid='view-all-cars']", attr="href")
    if dealer_url and dealer_url.startswith("/"):
        dealer_url = "https://dubai.dubizzle.com" + dealer_url

    contract_list = []
    for contract in ["daily", "weekly", "monthly"]:
        price = safe_select(f"h5[data-testid='rental-price-{contract}']")
        if not price:
            continue
        unlimited = safe_select(f"p[data-testid='unlimited-kms-{contract}']").lower() == "unlimited kilometers"
        raw_km = safe_select(f"p[data-testid='allowed-kms-{contract}']")
        km_match = re.search(r"\d+\s*km", raw_km, re.I) if raw_km else None
        km_limit = km_match.group() if km_match else None
        extra_km = safe_select(f"p[data-testid='additional-kms-{contract}']")
        contract_list.append({
            "contract": contract,
            "base_price": extract_numeric(price),
            "mileage": "Unlimited" if unlimited else fix_spacing(km_limit),
            "mileage_note": "" if unlimited else fix_spacing(extra_km)
        })

    # Extract general listing details
    description = safe_select("h6[data-testid='listing-sub-heading']")
    sub_description = safe_select("p[data-testid='description']")
    posted_on = safe_select("p[data-testid='posted-on']")
    dealer_name = safe_select("p[data-testid='name']")
    dealer_type = safe_select("p[data-testid='type']")
    min_age = safe_select("[data-ui-id='details-value-minimum_driver_age']")
    deposit = extract_numeric(safe_select("[data-ui-id='details-value-security_deposit']"))
    refund = safe_select("[data-ui-id='details-value-security_refund_period']")
    loc = safe_select("div[data-testid='listing-location-map']")

    # Combine contracts with metadata
    for entry in contract_list:
        enriched.append({
            "sub-url": url,
            "description": description,
            "sub_description": sub_description,
            "posted_on": posted_on,
            "dealer_name": dealer_name,
            "dealer_type": dealer_type,
            "dealer_page": dealer_url,
            **entry,
            "minimum_driver_age": min_age,
            "deposit": deposit,
            "refund_period": refund,
            "location": loc
        })

    return enriched

async def main():
    browser, context = await make_fast_firefox_async()
    semaphore = asyncio.Semaphore(5)
    tasks = []
    main_dataframes, detail_dicts, all_logs = [], [], []
    unique_config = config.drop_duplicates(subset=["make", "dubizzle_model"])
    async def scrape_task(make, model):
        logger = ThreadLogger()
        logger.set_context(f"{make.upper()}-{model.upper()}")
        await semaphore.acquire()
        try:
            page = await context.new_page()
            list_url = f"https://dubai.dubizzle.com/motors/rental-cars/{make}/{model}"
            logger.log(f"Scraping: {list_url}", emoji="🌐")
            df_main = await scrape_dubizzle_car_data_async(page, list_url)
            if df_main.empty:
                return [], [], logger.get_logs()
            merge_cols = ["make", "model", "year"]
            df_norm = df_main.copy()
            config_norm = config.rename(columns={"dubizzle_model": "model"}).copy()
            for col in merge_cols:
                df_norm[col] = df_norm[col].astype(str).str.upper()
                config_norm[col] = config_norm[col].astype(str).str.upper()
            filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")
            filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
            if filtered_df.empty:
                return [], [], logger.get_logs()
            enriched = []
            for _, row in filtered_df.iterrows():
                try:
                    detail = await scrape_dubizzle_detail_async(page, row["sub-url"])
                    enriched.extend(detail)
                except Exception as e:
                    logger.log(f"Detail error: {e}", indent=2)
            await page.close()
            return [filtered_df], enriched, logger.get_logs()
        finally:
            semaphore.release()
    for _, row in unique_config.iterrows():
        tasks.append(scrape_task(row["make"], row["dubizzle_model"]))
    results = await asyncio.gather(*tasks)
    await context.close()
    await browser.close()
    for main_df, detail, logs in results:
        main_dataframes.extend(main_df)
        detail_dicts.extend(detail)
        all_logs.extend(logs)
    print("\n📄 Log Summary:\n" + "="*40)
    for line in all_logs:
        print(line)
    if not main_dataframes:
        print("❌ No data found.")
        return
    main_df = pd.concat(main_dataframes, ignore_index=True)
    detail_df = pd.DataFrame(detail_dicts)
    mg_models = {"mg3": "3", "mg5": "5"}
    model_cleaned = main_df["model"].str.lower().replace(mg_models)
    main_df["title"] = (
        main_df["make"].str.lower() + " " +
        model_cleaned + " " +
        main_df["year"].astype(str)
    )
    if "base_price" in detail_df.columns:
        detail_df["savings"] = 0
        detail_df["offered_price"] = detail_df["base_price"]
    final_df = pd.merge(main_df, detail_df, on="sub-url", how="left")
    final_df["contract"] = final_df["contract"].astype(contract_order)
    df_sorted = final_df.sort_values(by=["contract", "sub-url"]).reset_index(drop=True)
    output_df = df_sorted[[
        "sub-url", "title", "make", "model", "year", "is_featured", "variant",
        "contract", "base_price", "savings", "offered_price", "description", "sub_description",
        "posted_on", "dealer_name", "dealer_type", "dealer_page", "mileage", "mileage_note",
        "minimum_driver_age", "deposit", "refund_period", "location"
    ]]
    output_df.to_excel(filename, index=False)
    print(f"\n✅ Saved: {filename}")

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())