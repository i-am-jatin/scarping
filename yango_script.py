from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import unquote
from bs4 import BeautifulSoup
import traceback
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
config = pd.read_csv(config_path, usecols=["make", "year", "yango_model"], low_memory=False)

# Filter out rows where yango_model is blank or NaN
config = config[config["yango_model"].notna() & (config["yango_model"].str.strip() != "")]

today_str = datetime.today().strftime("%d%m%Y")
filename = Path.cwd() / f"output/yango_rentals_{today_str}.xlsx"

# ========== TIME CALCULATION ==========
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

# ========== BROWSER FACTORY ==========
def make_fast_firefox(headless=True):
    options = Options()
    options.headless = headless
    options.page_load_strategy = "eager"
    options.add_argument("-private")

    options.set_preference("permissions.default.image", 2)
    options.set_preference("dom.ipc.processCount", 1)
    options.set_preference("browser.tabs.remote.autostart", False)
    options.set_preference("network.dns.disablePrefetch", True)
    options.set_preference("network.http.use-cache", False)
    options.set_preference("toolkit.cosmeticAnimations.enabled", False)
    options.set_preference("layout.css.animation.enabled", False)
    options.set_preference("layout.css.transition.enabled", False)
    options.set_preference("general.smoothScroll", False)
    options.set_preference("ui.prefersReducedMotion", 1)

    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
    service = Service(executable_path=r"C:\drivers\geckodriver.exe")
    
    return webdriver.Firefox(service=service, options=options)

# ========== DATA CLEANING ==========
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

def scroll_to_bottom(driver, pause=2, max_attempts=3):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for attempt in range(max_attempts):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Define custom sort orders
contract_order = CategoricalDtype(["daily", "weekly", "monthly"], ordered=True)
duration_order = CategoricalDtype(["1 month", "1 months" , "3 months", "6 months", "9 months", "12 months"], ordered=True)

# ========== MAIN PAGE SCRAPER ==========
def scrape_yango_car_data(driver, url, mode, logger):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)

    # Wait for the main container to 
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#Card")))
    except:
        print(f"⚠️ Timeout waiting for listings on {url}")
        return pd.DataFrame()

    # Scroll to bottom to load all listings
    scroll_to_bottom(driver)
    time.sleep(5)

    # Check if page has "No results" or "No matches"
    soup = BeautifulSoup(driver.page_source, "html.parser")
    heading_tags = soup.select("p.Heading_Title__WG8ox")

    for tag in heading_tags:
        text = tag.get_text(strip=True).lower()
        if "no results found" in text or "no matches found" in text:
            logger.log(f"🚫 ❌ Skipping broken main-url: {url}")
            return pd.DataFrame([{
                "sub-url": url,
                "page_status": "not found"
            }])

    # Extract make and model from URL
    make, model = extract_make_model_from_yango_url(url)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)

    car_data = []
    cars = driver.find_elements(By.CSS_SELECTOR, "#Card")

    for car in cars:
        try:
            link_elem = car.find_element(By.XPATH, "./ancestor::a[1]")
            car_url = link_elem.get_attribute("href")
            car_url = clean_booking_url(car_url)
            # if mode != "monthly":
                

            header_spans_top = car.find_elements(By.CSS_SELECTOR, "div.Card_LabelWrapper__zUzUR span")
            header_spans_bottom = car.find_elements(By.CSS_SELECTOR, "div.Card_LabelWrapperBottom__1XVgY span")
            header = get_unique_labels(header_spans_top + header_spans_bottom)

            year_type_text = car.find_element(By.CSS_SELECTOR, "span.ButtonSimilarInfo_ButtonSimilarInfoPrefix___Qou3").text.strip()
            desc_spans = car.find_elements(By.CSS_SELECTOR, "span.Card_CardBubble__zukT3")
            description = ", ".join([span.text.strip() for span in desc_spans if span.text.strip()])

            rating_spans = car.find_elements(By.CSS_SELECTOR, "div.Card_rating_wrapper__L_cLw span")
            ratings = " ".join([span.text.strip() for span in rating_spans if span.text.strip()])

            year_type_text_cleaned = re.search(r"\d{4}", year_type_text)
            year = int(year_type_text_cleaned.group()) if year_type_text_cleaned else None

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
            logger.log("[ERROR] Skipping a car:", e)
            continue

    return pd.DataFrame(car_data)

# ========== DETAIL PAGE SCRAPER ==========
def extract_subscription_details(driver, url, contract, logger):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)
    
    # Wait for the main container to 
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR, ".SlotText_Title__gHEmU, .BookFormSuggestedMonths_priceDetails__DoQLL")))
    except:
        print(f"⚠️ Timeout waiting for listings on {url}")
        return [{
            "sub-url": url,
            "page_status": "not found"
        }]

    # Scroll to bottom to load all listings
    scroll_to_bottom(driver)
    time.sleep(5)    

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    error_heading = soup.select_one("h1.Heading_Title__WG8ox") or soup.select_one("h1")

    if error_heading and any(msg in error_heading.get_text(strip=True).lower() for msg in ["page not found", "not available", "404"]):
        logger.log(f"❌ Skipping broken sub-url: {url}")
        return [{
            "sub-url": url,
            "page_status": "not found"
        }]

    def get_durations():
        duration_blocks = soup.find_all("div", class_="Slot_Slot__qIIVX")
        results = []
        for block in duration_blocks:
            title_div = block.select_one(".SlotText_Title__gHEmU")
            duration = title_div.get_text(strip=True).replace('\xa0', ' ') if title_div else None
            if not duration or not re.search(r"\d+\s*(day|week|month)", duration, re.IGNORECASE):
                continue
            if contract == "monthly":
                total_sub = block.select_one(".BookFormSuggestedMonths_withoutDiscount__zPhJI .SlotText_Subtitle__yHTPE")
                monthly_title = block.select_one(".BookFormSuggestedMonths_priceDetails__DoQLL .SlotText_Title__gHEmU")
                savings_sub = block.select_one(".BookFormSuggestedMonths_priceDetails__DoQLL .SlotText_Subtitle__yHTPE")
                offered_price = extract_numeric(monthly_title.get_text(strip=True)) if monthly_title else None
                base_price = extract_numeric(total_sub.get_text(strip=True)) if total_sub else None
                savings = extract_numeric(savings_sub.get_text(strip=True)) if savings_sub else None
                results.append({
                    "duration": duration,
                    "base_price": base_price,
                    "savings": savings,
                    "offered_price": offered_price
                })
            else:
                discounted_span = block.select_one(".Price_discounted__De4vH .Price_Value__ipyGJ")
                original_span = block.select_one(".Price_discounted__De4vH .Price_PriceNotDiscounted__0a3zc")
                duration_val = extract_numeric(duration)
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
        blocks = soup.find_all("div", class_="Slot_Slot__qIIVX")
        for block in blocks:
            title_divs = block.select(".SlotText_Title__gHEmU")
            if not title_divs:
                continue
            if any("mileage" in div.get_text(strip=True).lower() for div in title_divs):
                subtitle_div = block.find("div", class_="SlotText_Subtitle__yHTPE")
                raw_text = title_divs[-1].get_text(strip=True)
                
                km_match = re.search(r"[\d,]+\s*km", raw_text, re.IGNORECASE)
                mileage_value = km_match.group().replace(",", "") if km_match else None                
                
                result["mileage"] = mileage_value
                result["mileage_note"] = extract_amount_per_km(subtitle_div.get_text(strip=True)) if subtitle_div else None
                break
        return result

    def get_fuel_policy():
        result = {}
        blocks = soup.find_all("div", class_="Slot_Slot__qIIVX")
        for block in blocks:
            title_divs = block.select(".SlotText_Title__gHEmU")
            if not title_divs:
                continue
            if any("fuel policy" in div.get_text(strip=True).lower() for div in title_divs):
                result["fuel_policy"] = title_divs[-1].get_text(strip=True)
                break
        return result

    def get_deposit():
        result = {"base_deposit": None, "offered_deposit": None}
        blocks = soup.find_all("div", class_="Slot_Slot__qIIVX")
        for block in blocks:
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
                            deposit_value = extract_numeric(title_divs[i + 1].get_text(strip=True))
                            result["base_deposit"] = deposit_value
                            result["offered_deposit"] = deposit_value
                        break
            break
        return result

    def get_other_info():
        result = {}
        blocks = soup.find_all("div", class_="Island_Island__ap3Xw")
        for block in blocks:
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
                        raw_text = right_div.get_text(strip=True)
                        age_value = re.sub(r"\s*y\.?o\.?", " years", raw_text, flags=re.IGNORECASE)
                    
                        result["minimum_driver_age"] = age_value
                elif "driving experience" in label:
                    right_div = slot.select_one(".SlotText_right__alLBu .SlotText_Title__gHEmU")
                    result["minimum_driving_experience"] = right_div.get_text(strip=True).replace("y.o.", "years") if right_div else None
            break
        return result

    def get_insurance():
        result = {"insurance_type": None, "insurance_detail": None}
        type_list = []
        detail_list = []

        titles = soup.select(".SlotText_Title__gHEmU")

        # print(f"🔍 Total insurance title blocks found: {len(titles)}")

        for i, title_div in enumerate(titles, 1):
            title = title_div.get_text(strip=True)
            subtitle_div = title_div.find_next("div", class_="SlotText_Subtitle__yHTPE")
            subtitle = subtitle_div.get_text(" ", strip=True) if subtitle_div else ""

            # Only process titles containing keywords
            if any(k in title.lower() for k in ["insurance", "cover", "comprehensive"]):
                # print(f"✅ Block {i}: Title={title}, Subtitle={subtitle}")
                type_list.append(title)
                detail_list.append(subtitle or "")

        if type_list:
            result["insurance_type"] = ", ".join(type_list)
        if detail_list:
            result["insurance_detail"] = ", ".join(detail_list)

        return result

    durations = get_durations()
    mileage_options = get_mileage()
    fuel_policy = get_fuel_policy()
    deposit = get_deposit()
    insurance = get_insurance()
    other_info = get_other_info()

    enriched_data = []
    for duration_entry in durations:
        enriched_data.append({
            "sub-url": url,
            "page_status": "found",
            **duration_entry,
            **mileage_options,
            **fuel_policy,
            **deposit,
            **insurance,
            **other_info
        })

    return enriched_data

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

# ========== PARALLEL SCRAPE WRAPPER ==========

def scrape_main_for_make_model(make, model, logger):
    context = f"{make.upper()}-{model.upper()}"
    logger.set_context(context)
    logger.log(f"Started scraping for Make: {make}, Model: {model}", 0, "🚘")

    urls = {
        "daily": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until}",
        "weekly": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until_weekly}",
        "monthly": f"https://drive.yango.com/search/all/{make}/{model}?since={since}&until={until}&duration_months=9&is_monthly=true"
    }

    driver = make_fast_firefox(headless=True)
    local_main_dataframes, local_detail_dicts, local_seen_urls, broken_urls = [], [], set(), []

    try:
        for mode, url in urls.items():
            df = scrape_yango_car_data(driver, url, mode=mode, logger=logger)
            
            if df.empty or "page_status" not in df.columns:
                logger.log(f"⚠️ No {mode} results found, skipping.")
                continue
            
            logger.log(f"[INFO] Loaded {len(df)} cars from {mode.capitalize()}: {url}")
            
            # Filter the un-broken urls
            found_df = df[df["page_status"] == "found"].drop(columns=["page_status"])

            # Drop the unwanted urls based on make, model and year as defined in Config
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

            # Force type
            filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
            
            # Title
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
                    detail_df = extract_subscription_details(driver, sub_url, contract, logger=logger)
                    local_detail_dicts.extend(detail_df)
                    
                    if ( not detail_df or all("page_status" in d and d["page_status"] == "not found" for d in detail_df)):
                        logger.log(f"🚫 Detected sub-url broken. Skipping missing duration: {sub_url}")
                        broken_urls.append(sub_url)
                        continue
                    
                    if contract == "monthly":
                        found_groups = set()
                        for d in detail_df:
                            group = get_duration_group(extract_numeric(d.get("duration")))
                            if group:
                                found_groups.add(group)

                        missing_groups = sorted(set(duration_groups.keys()) - found_groups)
                        
                        for group in missing_groups:
                            rep_duration = duration_groups[group][0]
                            alt_url = re.sub(r"(duration_months=)\d+", lambda m: f"{m.group(1)}{rep_duration}", sub_url)

                            if alt_url in local_seen_urls:
                                logger.log(f"           🔁 Skipping duplicate group URL: {alt_url}")
                                continue
                            local_seen_urls.add(alt_url)
                            logger.log(f"        🔁 Missing group {group} → duration={rep_duration} → {alt_url}")

                            try:
                                alt_detail = extract_subscription_details(driver, alt_url, contract, logger=logger)
                                local_detail_dicts.extend(alt_detail)
                            except Exception as e:
                                logger.log(f"❌ Error scraping missing group URL: {e}")
                                continue
                except Exception as e:
                    logger.log(f"❌ Error scraping sub-page: {e}")
                    continue
    finally:
        driver.quit()
        logger.log("\n🛑 Browser closed.")
    return local_main_dataframes, local_detail_dicts, logger.get_logs()


# ========== MAIN RUN WITH THREADING ==========

if __name__ == "__main__":
    print("🚀 Launching parallel scraping...")

    main_dataframes, detail_dicts, all_logs = [], [], []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        unique_config = config.drop_duplicates(subset=['make', 'yango_model'])
        for _, row in unique_config.iterrows():
            make = row['make']
            model = row['yango_model']
            logger = ThreadLogger()
            futures.append(executor.submit(scrape_main_for_make_model, make, model, logger))

        for future in as_completed(futures):
            try:
                local_main_df, local_detail_df, logs = future.result()
                main_dataframes.extend(local_main_df)
                detail_dicts.extend(local_detail_df)
                all_logs.extend(logs)  # ⬅️ all logs from this thread
            except Exception as e:
                print(f"❌ Thread failed for make={make}, model={model}: {e}")
                traceback.print_exc()  # ✅ See full error

    # ========== POST PROCESSING ==========
    print("\n📄 Full Log Summary:\n" + "="*40)
    for line in all_logs:
        print(line)

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

    duration_set = set([1, 3, 6, 9, 12])
    monthly_groups = monthly_df.groupby("sub-url")
    new_rows = []

    for sub_url, group in monthly_groups:
        existing_durations = set(group["duration_num"].dropna().astype(int))
        missing = duration_set - existing_durations
        if not group.empty and missing:
            base_row = group.iloc[0]
            for m in missing:
                row = base_row.copy()
                row["duration_num"] = m
                row["duration"] = f"{m} months"
                new_rows.append(row)

    if new_rows:
        monthly_df = pd.concat([monthly_df, pd.DataFrame(new_rows)], ignore_index=True)

    monthly_df = monthly_df[monthly_df["duration_num"].isin(duration_set)]
    final_df = pd.concat([monthly_df, other_df], ignore_index=True)

    duration_pattern = re.compile(r"duration_months=\d+")
    final_df["sub-url"] = final_df.apply(
        lambda row: duration_pattern.sub(f"duration_months={int(row['duration_num'])}", row["sub-url"])
        if "duration_num" in row and pd.notnull(row["duration_num"]) else row["sub-url"],
        axis=1
    )

    # Drop unnecssary columns
    final_df.drop(columns=["duration_num", "object_id"], inplace=True)
    
    # Filter and process monthly contracts
    monthly = final_df[final_df["contract"] == "monthly"].copy()
    monthly["contract"] = monthly["contract"].astype(contract_order)
    monthly["duration"] = monthly["duration"].astype(duration_order)
    monthly_sorted = monthly.sort_values(by=["contract", "sub-url", "duration"]).reset_index(drop=True)

    # Filter and process non-monthly contracts
    non_monthly = final_df[final_df["contract"] != "monthly"].copy()
    non_monthly["contract"] = non_monthly["contract"].astype(contract_order)
    non_monthly_sorted = non_monthly.sort_values(by=["contract", "sub-url"]).reset_index(drop=True)

    # Combine and sort again by contract and sub-url
    df_sorted = pd.concat([non_monthly_sorted, monthly_sorted]).sort_values(by=["contract", "sub-url"]).reset_index(drop=True)
        
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