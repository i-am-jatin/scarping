from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import unquote
import pandas as pd
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
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
config = pd.read_csv(config_path, usecols=["make", "year", "invygo_model"], low_memory=False)

# Filter out rows where invygo_model is blank or NaN
config = config[config["invygo_model"].notna() & (config["invygo_model"].str.strip() != "")]

today_str = datetime.today().strftime("%d%m%Y")
filename = Path.cwd() / f"output/invygo_rentals_{today_str}.xlsx"

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
contract_order = CategoricalDtype(["weekly", "monthly"], ordered=True)
duration_order = CategoricalDtype(["1 week", "1 month", "3 months", "6 months", "9 months"], ordered=True)

# ========== MAIN PAGE SCRAPER ==========

def scrape_invigo_car_data(driver, url, mode):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)

    # Wait until the cards are loaded
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^='/en-ae/dubai/rent-'] div.p-4"))
        )
    except:
        print(f"⚠️ Timeout waiting for listings on {url}")
        return pd.DataFrame()

    # Scroll to bottom to load all listings
    scroll_to_bottom(driver)
    time.sleep(5)

    # Parse final DOM
    soup = BeautifulSoup(driver.page_source, "html.parser")
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

def extract_subscription_details(driver, url):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)

    try:
        # Wait until at least one duration option is visible
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="booking-contract-length"] [role="presentation"]'))
        )
    except:
        print(f"❌ Timeout: Contract durations not loaded at {url}")
        return []
    
    # Scroll to bottom to load all listings
    scroll_to_bottom(driver) 
    time.sleep(5)  

    enriched_data = []

    duration_elements = driver.find_elements(By.CSS_SELECTOR, '[data-testid="booking-contract-length"] [role="presentation"]')

    for index, elem in enumerate(duration_elements):
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", elem)
            time.sleep(0.3)
            elem.click()
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            duration_block = soup.select('[data-testid="booking-contract-length"] [role="presentation"]')[index]
            duration_text = duration_block.select_one('div.text-cool-gray-900').text.strip()
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

# ========== PARALLEL SCRAPE WRAPPER ==========

def scrape_main_for_mode(mode, logger):
    context = f"{mode.upper()}"
    logger.set_context(context)
    
    global config
    urls = {
        "weekly": "https://www.invygo.com/en-ae/dubai/rent-weekly-cars",
        "monthly": "https://www.invygo.com/en-ae/dubai/rent-monthly-cars"
    }

    url = urls[mode]
    driver = make_fast_firefox(headless=True)
    local_main_dataframes, local_detail_dicts = [], []

    try:
        df = scrape_invigo_car_data(driver, url, mode=mode)

        if df.empty:
            logger.log(f"⚠️ No {mode} results found, skipping.")
            return [], [], logger.get_logs()

        logger.log(f"[INFO] Loaded {len(df)} cars from {mode.capitalize()}: {url}")
        df[["make", "model"]] = df["sub-url"].apply(lambda url: pd.Series(extract_make_model_from_url(url)))

        merge_cols = ["make", "model", "year"]
        df_norm = df.copy()
        missing_cols = [col for col in merge_cols if col not in df_norm.columns]
        if missing_cols:
            logger.log(f"❌ Skipping {mode} — missing columns: {missing_cols}")
            return [], [], logger.get_logs()

        for col in merge_cols:
            df_norm[col] = df_norm[col].astype(str).str.upper()
        config_norm = config.rename(columns={"invygo_model": "model"}).copy()
        for col in merge_cols:
            config_norm[col] = config_norm[col].astype(str).str.upper()
        filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")

        filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
        filtered_df['title'] = filtered_df['title'].str.lower() + ' ' + filtered_df['year'].astype(str)

        if filtered_df.empty:
            logger.log(f"⚠️ No {mode} results matched config.")
            return [], [], logger.get_logs()

        local_main_dataframes.append(filtered_df)
        logger.log(f"   🔢 Filtered {mode} cars: {len(filtered_df)}")

        for _, car_row in filtered_df.iterrows():
            sub_url = car_row["sub-url"]
            logger.log(f"     ↪️ Sub-page: {sub_url}")
            try:
                detail_df = extract_subscription_details(driver, sub_url)
                local_detail_dicts.extend(detail_df)
            except Exception as e:
                logger.log(f"❌ Error scraping sub-page: {e}")
    finally:
        driver.quit()
        logger.log(f"🛑 Browser closed for {mode}.")

    return local_main_dataframes, local_detail_dicts, logger.get_logs()

# ========== MAIN RUN WITH THREADING ==========

if __name__ == "__main__":
    print("🚀 Launching parallel scraping for weekly and monthly...")

    main_dataframes, detail_dicts, all_logs = [], [], []
    modes = ["weekly", "monthly"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for mode in modes:
            logger = ThreadLogger()
            futures.append(executor.submit(scrape_main_for_mode, mode, logger))

        for future in as_completed(futures):
            try:
                local_main_df, local_detail_df, logs = future.result()
                main_dataframes.extend(local_main_df)
                detail_dicts.extend(local_detail_df)
                all_logs.extend(logs)
            except Exception as e:
                print(f"❌ Thread failed: {e}")
                traceback.print_exc()

    print("\n📄 Full Log Summary:\n" + "=" * 40)
    for line in all_logs:
        print(line)

    main_df = pd.concat(main_dataframes, ignore_index=True)
    detail_df = pd.DataFrame(detail_dicts)
    
    if not detail_df.empty:

        # Extract numeric duration as integer (from '1 week', '9 months', etc.)
        duration_num = detail_df["duration"].str.extract(r"(\d+)")[0].astype("Int64")

        # Calculate base price
        detail_df["base_price"] = (detail_df["savings"] / duration_num) + detail_df["offered_price"]

        # Move mileage out first (pop removes the column)
        if 'mileage_numeric' in detail_df.columns:
            mileage = detail_df.pop("mileage_numeric")
            detail_df["base_price"] += mileage
            detail_df["offered_price"] += mileage

    # Merge
    final_df = pd.merge( main_df, detail_df, on="sub-url", how="left" )

    # Apply the categorical types
    final_df["contract"] = final_df["contract"].astype(contract_order)
    final_df["duration"] = final_df["duration"].astype(duration_order)

    # Sort the DataFrame correctly
    df_sorted = final_df.sort_values(by=["contract", "sub-url", "duration", "mileage"]).reset_index(drop=True)

    # Reorder columns
    invygo_df = df_sorted[[
    "sub-url", "title", "make", "model", "year", "promotion", "runnings_kms",
    "contract", "base_price", "savings", "offered_price", "duration", "mileage",
    "mileage_note", "standard_cover_insurance", "full_cover_insurance"]]

    # Export as Excel
    invygo_df.to_excel(filename, index=False)
    print(f"\n📁 Saved: {filename}")
    print("\n✅ All scraping complete.")