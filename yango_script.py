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
import traceback
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import threading
from pandas.api.types import CategoricalDtype
import pytz

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
config = pd.read_csv(config_path, usecols=["make", "year", "yango_model"], low_memory=False)
config = config[config["yango_model"].notna() & (config["yango_model"].str.strip() != "")]
filename = Path.cwd() / f"output/yango_rentals.xlsx"    

ist = pytz.timezone('Asia/Kolkata')
from_date = datetime.now(ist).date() + timedelta(days=1)
to_date = from_date + timedelta(days=1)
to_date_weekly = from_date + timedelta(days=7)

since = int((ist.localize(datetime(from_date.year, from_date.month, from_date.day, 11, 30, 0))
             .astimezone(pytz.utc)).timestamp() * 1000)
until = int((ist.localize(datetime(to_date.year, to_date.month, to_date.day, 11, 30, 0))
             .astimezone(pytz.utc)).timestamp() * 1000)
until_weekly = int((ist.localize(datetime(to_date_weekly.year, to_date_weekly.month, to_date_weekly.day, 11, 30, 0))
             .astimezone(pytz.utc)).timestamp() * 1000)

def extract_numeric(text):
    if pd.isnull(text):
        return 0
    nums = ''.join(filter(str.isdigit, text))
    return int(nums) if nums else 0

def extract_amount_per_km(text):
    match = re.search(r"\b\d+(?:\.\d+)?\sper\skm", text, re.IGNORECASE)
    return match.group(0) if match else None

def get_unique_labels(spans):
    return ", ".join(sorted(set(span.text.strip() for span in spans if span.text.strip())))

def clean_booking_url(url):
    return re.sub(r"&duration_months=\d+", "", url)

def extract_make_model_from_yango_url(url):
    url = unquote(url)
    match = re.search(r'/search/all/([^/]+)/([^/?#]+)', url)
    if match:
        make = match.group(1)
        model = match.group(2)
        return make, model
    return None, None

contract_order = CategoricalDtype(["daily", "weekly", "monthly"], ordered=True)
duration_order = CategoricalDtype(
    categories=["1 month", "3 months", "6 months", "9 months", "12 months"],
    ordered=True
)

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
    return playwright, browser, context

async def scroll_to_bottom_async(page, pause=2, max_attempts=3):
    for _ in range(max_attempts):
        prev_height = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            break
        
async def scrape_yango_car_data(page, url, mode, logger):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")
    await page.wait_for_selector("#Card", timeout=10000)
    await scroll_to_bottom_async(page)
    await asyncio.sleep(2)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    
    heading_tags = soup.select("p.Heading_Title__WG8ox")
    for tag in heading_tags:
        text = tag.get_text(strip=True).lower()
        if "no results found" in text or "no matches found" in text:
            logger.log(f"🚫 ❌ Skipping broken main-url: {url}")
            return pd.DataFrame([{"sub-url": url, "page_status": "not found"}])

    make, model = extract_make_model_from_yango_url(url)
    
    car_data = []
    car_cards = await page.query_selector_all("#Card")
    for car in car_cards:
        try:
            car_url = await car.evaluate("el => el.closest('a')?.href")
            if car_url:
                car_url = clean_booking_url(car_url)
            
            # Header
            header_spans = await car.query_selector_all("div[class*='LabelWrapper'] span")
            headers = [(await span.text_content() or "").strip() for span in header_spans]
            header = ", ".join(sorted(set(h for h in headers if h)))

            # Description
            desc_spans = await car.query_selector_all("span[class*='CardBubble']")
            description = ", ".join([(await s.text_content() or "").strip() for s in desc_spans if (await s.text_content() or "").strip()])

            # Ratings
            rating_spans = await car.query_selector_all("div[class*='rating'] span")
            ratings = " ".join([(await s.text_content() or "").strip() for s in rating_spans if (await s.text_content() or "").strip()])

            year_type_elem = await car.query_selector("span.ButtonSimilarInfo_ButtonSimilarInfoPrefix___Qou3")
            # print("📌 Found element:", bool(year_type_elem))
            if year_type_elem:
                year_type_text = (await year_type_elem.text_content() or "").strip()
                # print("📌 Year text:", year_type_text)
                year_match = re.search(r"\d{4}", year_type_text)
                year = int(year_match.group()) if year_match else None
            else:
                year = None

            car_data.append({
                "sub-url": "https://dubai.yango.com" + car_url.replace('&location=ae&sublocation=db', '') if car_url.startswith("/") else car_url.replace('&location=ae&sublocation=db', ''),
                "page_status": "found",
                "header": header,
                "make": make,
                "model": model,  
                "year": year,                              
                "description": description,
                "ratings": ratings,
                "contract": mode
            })
        except Exception as e:
            logger.log(f"❌ Error parsing car: {str(e)}")
            continue

    return pd.DataFrame(car_data)

async def extract_subscription_details(page, url, contract, logger):
    await page.goto(url, timeout=10000, wait_until="domcontentloaded")
    
    try:
        await page.wait_for_selector(".SlotText_Title__gHEmU, .BookFormSuggestedMonths_priceDetails__DoQLL", timeout=10000)
    except:
        print(f"⚠️ Timeout waiting for listings on {url}")
        return [{"sub-url": url, "page_status": "not found"}]
    
    await scroll_to_bottom_async(page)
    await asyncio.sleep(5)
    
    content = await page.content()
    soup = BeautifulSoup(content, "html.parser")

    # Check error page
    error_heading = soup.select_one("h1.Heading_Title__WG8ox") or soup.select_one("h1")
    if error_heading and any(msg in error_heading.get_text(strip=True).lower() for msg in ["page not found", "not available", "404"]):
        logger.log(f"❌ Skipping broken sub-url: {url}")
        return [{"sub-url": url, "page_status": "not found"}]

    # Inner scraping functions
    def get_durations():
        results = []
        for block in soup.find_all("div", class_="Slot_Slot__qIIVX"):
            title_div = block.select_one(".SlotText_Title__gHEmU")
            duration = title_div.get_text(strip=True).replace('\xa0', ' ') if title_div else None
            if not duration or not re.search(r"\d+\s*(day|week|month)", duration, re.IGNORECASE):
                continue
            if contract == "monthly":
                total_sub = block.select_one(".BookFormSuggestedMonths_withoutDiscount__zPhJI .SlotText_Subtitle__yHTPE")
                total = extract_numeric(total_sub.get_text(strip=True)) if total_sub else None

                monthly_title = block.select_one(".BookFormSuggestedMonths_priceDetails__DoQLL .SlotText_Title__gHEmU")
                offered = extract_numeric(monthly_title.get_text(strip=True)) if monthly_title else None

                savings_sub = block.select_one(".BookFormSuggestedMonths_priceDetails__DoQLL .SlotText_Subtitle__yHTPE")
                savings = extract_numeric(savings_sub.get_text(strip=True)) if savings_sub else None
                
                base_price = offered if offered is not None and total is None else total
                results.append({
                    "duration": duration,
                    "base_price": base_price,
                    "savings": savings if savings is not None else 0,
                    "offered_price": offered,
                })
            else:
                duration_val = extract_numeric(duration)

                # Try discounted + original (usual case)
                discounted_span = block.select_one(".Price_discounted__De4vH .Price_Value__ipyGJ")
                original_span = block.select_one(".Price_discounted__De4vH .Price_PriceNotDiscounted__0a3zc")

                # Fallback if original_span not found (i.e., no discount case)
                if original_span is None:
                    original_span = block.select_one(".Price_Value__ipyGJ.Price_rightGap___M1b_")
                    base = extract_numeric(original_span.get_text(strip=True)) if original_span else None

                    results.append({
                        "duration": duration,
                        "base_price": (base * duration_val) if base and duration_val else None,
                        "savings": 0,
                        "offered_price": base
                    })
                else:
                    offered = extract_numeric(discounted_span.get_text(strip=True)) if discounted_span else None
                    base = extract_numeric(original_span.get_text(strip=True)) if original_span else None

                    results.append({
                        "duration": duration,
                        "base_price": (base * duration_val) if base and duration_val else None,
                        "savings": ((base - offered) * duration_val) if base and offered and duration_val else None,
                        "offered_price": (offered * duration_val) if offered and duration_val else None
                    })
        return results

    def get_mileage():
        result = {}
        for block in soup.find_all("div", class_="Slot_Slot__qIIVX"):
            title_divs = block.select(".SlotText_Title__gHEmU")
            if title_divs and any("mileage" in div.get_text(strip=True).lower() for div in title_divs):
                subtitle_div = block.find("div", class_="SlotText_Subtitle__yHTPE")
                raw_text = title_divs[-1].get_text(strip=True)
                km_match = re.search(r"[\d,]+\s*km", raw_text, re.IGNORECASE)
                
                result["mileage"] = km_match.group().replace(",", "") if km_match else None
                result["mileage_note"] = extract_amount_per_km(subtitle_div.get_text(strip=True)) if subtitle_div else None
                break
        return result

    def get_fuel_policy():
        result = {}
        for block in soup.find_all("div", class_="Slot_Slot__qIIVX"):
            title_divs = block.select(".SlotText_Title__gHEmU")
            if title_divs and any("fuel policy" in div.get_text(strip=True).lower() for div in title_divs):
                result["fuel_policy"] = title_divs[-1].get_text(strip=True)
                break
        return result

    def get_deposit():
        result = {"base_deposit": None, "offered_deposit": None}
        for block in soup.find_all("div", class_="Slot_Slot__qIIVX"):
            title_divs = block.select(".SlotText_Title__gHEmU")
            if not title_divs or not any("deposit" in div.get_text(strip=True).lower() for div in title_divs):
                continue
            offer_div = block.select_one("span > p > span.Text_Text__F4Wpv.Text_size_M__E57lv")
            strike = block.select_one(".SlotText_strikethrough__3lJ4R .SlotText_Title__gHEmU")
            if strike and offer_div:
                result["base_deposit"] = extract_numeric(strike.get_text(strip=True))
                result["offered_deposit"] = extract_numeric(offer_div.get_text(strip=True))
            else:
                for i, div in enumerate(title_divs):
                    if "deposit" in div.get_text(strip=True).lower():
                        if i + 1 < len(title_divs):
                            val = extract_numeric(title_divs[i + 1].get_text(strip=True))
                            result["base_deposit"] = val
                            result["offered_deposit"] = val
                        break
            break
        return result

    def get_insurance():
        result = {"insurance_type": None, "insurance_detail": None}
        type_list, detail_list = [], []
        for div in soup.select(".SlotText_Title__gHEmU"):
            title = div.get_text(strip=True)
            subtitle_div = div.find_next("div", class_="SlotText_Subtitle__yHTPE")
            subtitle = subtitle_div.get_text(" ", strip=True) if subtitle_div else ""
            if any(k in title.lower() for k in ["insurance", "cover", "comprehensive"]):
                type_list.append(title)
                detail_list.append(subtitle or "")

        if type_list:
            result["insurance_type"] = ", ".join(type_list)
        if detail_list:
            result["insurance_detail"] = ", ".join(detail_list)

        return result

    def get_other_info():
        result = {}
        for block in soup.find_all("div", class_="Island_Island__ap3Xw"):
            slots = block.select(".BookFormImportantInfo_slot__apVPj")
            if not slots:
                continue                
            for slot in slots:
                title_div = slot.select_one(".SlotText_Title__gHEmU")
                if not title_div:
                    continue
                label = title_div.get_text(strip=True).lower()
                if "payment" in label:
                    titles = slot.select(".SlotText_Title__gHEmU")
                    if len(titles) >= 2:
                        result["payment_mode"] = titles[0].get_text(strip=True)
                        result["payment_options"] = titles[1].get_text(strip=True)
                elif "minimum age" in label:
                    right_div = slot.select_one(".SlotText_right__alLBu .SlotText_Title__gHEmU")
                    if right_div:
                        result["minimum_driver_age"] = re.sub(r"\s*y\.?o\.?", " years", right_div.get_text(strip=True), flags=re.IGNORECASE)
                elif "driving experience" in label:
                    right_div = slot.select_one(".SlotText_right__alLBu .SlotText_Title__gHEmU")
                    if right_div:
                        result["minimum_driving_experience"] = right_div.get_text(strip=True).replace("y.o.", "years")
            break
        return result

    durations = get_durations()
    mileage = get_mileage()
    fuel = get_fuel_policy()
    deposit = get_deposit()
    insurance = get_insurance()
    other = get_other_info()

    enriched_data = []

    for d in durations:
        enriched_data.append({
            "sub-url": url,
            "page_status": "found",
            **d, **mileage, **fuel, **deposit, **insurance, **other
        })

    return enriched_data
from playwright.async_api import async_playwright
import asyncio
import re
import pandas as pd

duration_groups = {
    "1": [1],
    "2-3": [2, 3],
    "4-6": [4, 5, 6],
    "7-9": [7, 8, 9],
    "10+": [10, 11, 12, 13]
}

def get_duration_group(duration):
    for group, values in duration_groups.items():
        if duration in values:
            return group
    return None

async def scrape_main_for_make_model(make, model, logger):
    context = f"{make.upper()}-{model.upper()}"
    logger.set_context(context)
    logger.log(f"Started scraping for Make: {make}, Model: {model}", 0, "🚘")

    urls = {
        "daily": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until}",
        "weekly": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until_weekly}",
        "monthly": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until}&duration_months=9&is_monthly=true"
    }

    local_main_dataframes, local_detail_dicts, local_seen_urls, broken_urls = [], [], set(), []

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            for mode, url in urls.items():
                df = await scrape_yango_car_data(page, url, mode=mode, logger=logger)

                if df.empty or "page_status" not in df.columns:
                    logger.log(f"⚠️ No {mode} results found, skipping.")
                    continue

                logger.log(f"[INFO] Loaded {len(df)} cars from {mode.capitalize()}: {url}")

                found_df = df[df["page_status"] == "found"].drop(columns=["page_status"])

                merge_cols = ["make", "model", "year"]
                df_norm = found_df.copy()

                missing_cols = [col for col in merge_cols if col not in df_norm.columns]
                if missing_cols:
                    logger.log(f"❌ Skipping {make}-{model} — missing columns: {missing_cols}")
                    return [], [], logger.get_logs()

                for col in merge_cols:
                    df_norm[col] = df_norm[col].astype(str).str.upper()
                config_norm = config.rename(columns={"yango_model": "model"}).copy()
                for col in merge_cols:
                    config_norm[col] = config_norm[col].astype(str).str.upper()
                filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")

                filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
                filtered_df['title'] = filtered_df['make'].str.lower() + ' ' + filtered_df['model'].str.lower() + ' ' + filtered_df['year'].astype(str)

                if filtered_df.empty:
                    logger.log(f"⚠️ No {mode} results found, inline with Config.")
                    continue

                local_main_dataframes.append(filtered_df)
                logger.log(f"   🔢 Filtered {make}, {model} cars: {len(filtered_df)}")

                for _, car_row in filtered_df.iterrows():
                    sub_url = car_row["sub-url"]
                    contract = car_row["contract"]

                    if sub_url in local_seen_urls:
                        logger.log(f"  🔁 Skipping already scraped: {sub_url}")
                        continue

                    local_seen_urls.add(sub_url)
                    logger.log(f"     ↪️ Opening sub-page: {sub_url}")

                    try:
                        detail_df = await extract_subscription_details(page, sub_url, contract, logger=logger)
                        local_detail_dicts.extend(detail_df)

                        if (not detail_df or all("page_status" in d and d["page_status"] == "not found" for d in detail_df)):
                            logger.log(f"🚫 Detected sub-url broken. Skipping missing duration: {sub_url}")
                            broken_urls.append(sub_url)
                            continue

                        if contract == "monthly":
                            found_groups = set()
                            for d in detail_df:
                                duration = extract_numeric(d.get("duration"))
                                group = get_duration_group(duration)
                                if group:
                                    found_groups.add(group)

                            missing_groups = sorted(set(duration_groups.keys()) - found_groups)

                            for group in missing_groups:
                                rep_duration = duration_groups[group][0]
                                if "duration_months=" in sub_url:
                                    alt_url = re.sub(r"(duration_months=)\d+", f"\\1{rep_duration}", sub_url)
                                else:
                                    alt_url = sub_url + f"&duration_months={rep_duration}"

                                if alt_url in local_seen_urls:
                                    logger.log(f"           🔁 Skipping duplicate group URL: {alt_url}")
                                    continue
                                
                                local_seen_urls.add(alt_url)
                                logger.log(f"        🔁 Missing group {group} → duration={rep_duration} → {alt_url}")

                                try:
                                    alt_detail = await extract_subscription_details(page, alt_url, contract, logger=logger)
                                    local_detail_dicts.extend(alt_detail)
                                except Exception as e:
                                    logger.log(f"❌ Error scraping missing group URL: {e}")
                                    continue
                    except Exception as e:
                        logger.log(f"❌ Error scraping sub-page: {e}")
                        continue
        finally:
            await browser.close()
            logger.log("\n🛑 Browser closed.")

    return local_main_dataframes, local_detail_dicts, logger.get_logs()

async def main():
    print("🚀 Launching parallel scraping...")

    main_dataframes, detail_dicts, all_logs = [], [], []

    async def run_single_scrape(make, model):
        logger = ThreadLogger()
        try:
            return await scrape_main_for_make_model(make, model, logger)
        except Exception as e:
            print(f"❌ Thread failed for make={make}, model={model}: {e}")
            traceback.print_exc()
            return [], [], logger.get_logs()

    # Run scrapes in parallel
    unique_config = config.drop_duplicates(subset=['make', 'yango_model'])
    tasks = []
    for _, row in unique_config.iterrows():
        make = row['make']
        model = row['yango_model']
        tasks.append(run_single_scrape(make, model))

    results = await asyncio.gather(*tasks)

    # Unpack results
    for local_main_df, local_detail_df, logs in results:
        main_dataframes.extend(local_main_df)
        detail_dicts.extend(local_detail_df)
        all_logs.extend(logs)

    # ========== POST PROCESSING ==========
    print("\n📄 Full Log Summary:\n" + "=" * 40)
    for line in all_logs:
        print(line)

    if not main_dataframes:
        print("❌ No main data to concatenate. Exiting.")
        return

    if not detail_dicts:
        print("❌ No detail data to process. Exiting.")
        return

    main_df = pd.concat(main_dataframes, ignore_index=True)
    detail_df = pd.DataFrame(detail_dicts)

    main_df["lookup"] = main_df["sub-url"].apply(clean_booking_url)
    detail_df["object_id"] = detail_df["sub-url"].str.extract(r"object_id=([a-f0-9\-]+)")
    cols_to_consider = detail_df.columns.difference(["sub-url", "filter"])
    detail_df = detail_df.drop_duplicates(subset=cols_to_consider)
    detail_df["lookup"] = detail_df["sub-url"].apply(clean_booking_url)

    final_df = pd.merge(main_df, detail_df.drop(["sub-url"], axis=1), on="lookup", how="left").drop(columns=["lookup"])
    final_df["duration_num"] = final_df["duration"].str.extract(r"(\d+)").fillna(0).astype(int)
    
    monthly_df = final_df[final_df["contract"] == "monthly"].copy()
    other_df = final_df[final_df["contract"] != "monthly"].copy()

    # Duration grouping
    months_groups = {
        "1 month": [1],
        "3 months": [2, 3],
        "6 months": [4, 5, 6],
        "9 months": [7, 8, 9],
        "12 months": [10, 11, 12, 13]
    }

    def get_month_group(month):
        for group, values in months_groups.items():
            if month in values:
                return group
        return None

    # Apply group mapping
    monthly_df["month_group"] = monthly_df["duration_num"].apply(get_month_group)

    monthly_df = (
        monthly_df
        .sort_values(["sub-url", "duration_num"])
        .drop_duplicates(subset=["sub-url", "month_group"], keep="first")
        .reset_index(drop=True)
    )

    # Update the duration column
    monthly_df["duration"] = monthly_df["month_group"]
    monthly_df.drop(columns=["month_group"], inplace=True)

    # Update 'sub-url' to reflect correct duration_months=X using duration in text
    def add_duration_to_url(row):
        url = row["sub-url"]
        duration = row["duration"]

        # Extract number from duration string (e.g., "10 Months" → 10)
        match = re.search(r"\d+", str(duration))
        if not match:
            return url  # No valid duration found, return original URL

        duration_num = match.group()
        
        if "duration_months=" in url:
            # Already present: replace it
            return re.sub(r"duration_months=\d+", f"duration_months={duration_num}", url)
        else:
            # Not present: append it
            separator = "&" if "?" in url else "?"
            return url + f"{separator}duration_months={duration_num}"

    # Apply to monthly_df
    monthly_df["sub-url"] = monthly_df.apply(add_duration_to_url, axis=1)

    # Sorting contract, sub-url, duration at monthly_df
    monthly_df["contract"] = monthly_df["contract"].astype(contract_order)
    monthly_df["duration"] = monthly_df["duration"].str.lower().astype(duration_order)
    monthly_sorteddf = monthly_df.sort_values(by=["contract", "object_id" , "duration" ]).reset_index(drop=True)

    # Sorting contract, sub-url at other_df
    other_df["contract"] = other_df["contract"].astype(contract_order)
    other_sorteddf = other_df.sort_values(by=["contract", "object_id"]).reset_index(drop=True)

    # Combine and sort again by contract and sub-url
    df_sorted = pd.concat([other_sorteddf, monthly_sorteddf]).sort_values(by=["contract", "object_id"]).reset_index(drop=True)

    # Drop unnecssary columns
    df_sorted.drop(columns=["duration_num", "object_id"], inplace=True)

    # Reorder columns
    yango_df = df_sorted[[
    "sub-url", "title", "make", "model", "year", "ratings", "header", "contract",
    "base_price", "savings", "offered_price", "duration", "mileage", "mileage_note",
    "insurance_type", "insurance_detail", "description", "page_status", "fuel_policy",
    "base_deposit", "offered_deposit", "payment_mode", "payment_options",
    "minimum_driver_age", "minimum_driving_experience"]]

    # Export as Excel
    yango_df.to_excel(filename, index=False)
    print(f"\n📁 Saved: {filename}")
    print("✅ All parallel scraping complete.")    

if __name__ == "__main__":
    asyncio.run(main())