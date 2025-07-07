from pathlib import Path
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
from bs4 import BeautifulSoup
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
config = pd.read_csv(config_path, usecols=["make", "year", "dubizzle_model"], low_memory=False)

# Filter out rows where dubizzle_model is blank or NaN
config = config[config["dubizzle_model"].notna() & (config["dubizzle_model"].str.strip() != "")]

# today_str = datetime.today().strftime("%d%m%Y")
# filename = Path.cwd() / f"output/dubizzle_rentals_{today_str}.xlsx"
filename = Path.cwd() / "output/dubizzle_rentals.xlsx"


# ========== BROWSER FACTORY ==========

def make_fast_firefox(headless=True):
    options = Options()
    options.headless = headless

    # Load pages as quickly as possible
    options.page_load_strategy = "eager"

    # Private browsing mode (no cookies/cache)
    options.add_argument("-private")

    # Reduce unnecessary rendering and animation
    options.set_preference("permissions.default.image", 2)  # block all images
    options.set_preference("media.autoplay.default", 0)  # allow autoplay (can reduce blocking)
    options.set_preference("browser.shell.checkDefaultBrowser", False)
    options.set_preference("browser.startup.page", 0)  # skip startup tabs
    options.set_preference("browser.startup.homepage", "about:blank")
    options.set_preference("startup.homepage_welcome_url", "about:blank")
    options.set_preference("startup.homepage_welcome_url.additional", "about:blank")

    # Disable smooth effects
    options.set_preference("toolkit.cosmeticAnimations.enabled", False)
    options.set_preference("layout.css.animation.enabled", False)
    options.set_preference("layout.css.transition.enabled", False)
    options.set_preference("general.smoothScroll", False)
    options.set_preference("ui.prefersReducedMotion", 1)

    # Disable prefetching and caching
    options.set_preference("network.dns.disablePrefetch", True)
    options.set_preference("network.prefetch-next", False)
    options.set_preference("network.http.use-cache", False)

    # Reduce tab/thread overhead
    options.set_preference("dom.ipc.processCount", 1)
    options.set_preference("browser.tabs.remote.autostart", False)

    # Optional: disable fonts (can break rendering, use with caution)
    options.set_preference("gfx.downloadable_fonts.enabled", False)

    # Set paths
    # options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
    # service = Service(executable_path=r"C:\drivers\geckodriver.exe")

    # return webdriver.Firefox(service=service, options=options)
    return webdriver.Firefox(options=options)
# ========== DATA CLEANING HELPERS ==========

def extract_numeric(text):
    if not text:
        return None
    nums = ''.join(filter(str.isdigit, text))
    return int(nums) if nums else None

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

# ========== MAIN PAGE SCRAPER ==========

def scrape_dubizzle_car_data(driver, url):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)

    # Wait for car listings to appear
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#listing-card-wrapper a[data-testid^='listing-']"))
        )
    except:
        print(f"⚠️ Timeout waiting for listings on {url}")
        return pd.DataFrame()
    
    # Scroll to bottom to load all listings
    scroll_to_bottom(driver)
    time.sleep(5)

    # Parse final DOM
    soup = BeautifulSoup(driver.page_source, "html.parser")
    car_cards = soup.select("#listing-card-wrapper a[data-testid^='listing-']")

    extracted = []

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

            extracted.append({
                "sub-url": full_url,
                "make": car_name,
                "model": model,
                "variant": variant,
                "year": year,
                "is_featured": is_featured
            })

        except Exception as e:
            print("❌ Error parsing card:", e)
            continue

    return pd.DataFrame(extracted)


# ========== DETAIL PAGE SCRAPER ==========

def scrape_dubizzle_detail(driver, url):
    driver.get(url)
    
    # Set implicit wait for soft fallback
    driver.implicitly_wait(5)

    # Wait for description or contract section to load
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h6[data-testid='listing-sub-heading'], h5[data-testid^='rental-price-']"))
        )
    except:
        print(f"⚠️ Timeout waiting for detail elements {url}.")
        return []


    # Scroll to bottom to load all listings
    scroll_to_bottom(driver)
    time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    enriched_data = []

    def safe_select(selector, attr="text", many=False):
        try:
            elements = soup.select(selector)
            if not elements:
                return "" if not many else []
            if attr == "text":
                return [el.get_text(strip=True) for el in elements] if many else elements[0].get_text(strip=True)
            else:
                return [el.get(attr, "") for el in elements] if many else elements[0].get(attr, "")
        except Exception as e:
            print(f"⚠️ safe_select failed for {selector}: {e}")
            return "" if not many else []

    # Dealer link
    dealer_url = safe_select("a[data-testid='view-all-cars']", attr="href")
    if dealer_url and dealer_url.startswith("/"):
        dealer_url = "https://dubai.dubizzle.com" + dealer_url

    contract_list = []
    for contract in ["daily", "weekly", "monthly"]:
        price = safe_select(f"h5[data-testid='rental-price-{contract}']")
        if not price:
            continue

        unlimited_text = safe_select(f"p[data-testid='unlimited-kms-{contract}']").lower()
        unlimited = unlimited_text == "unlimited kilometers"

        raw_km = safe_select(f"p[data-testid='allowed-kms-{contract}']")
        km_match = re.search(r"\d+\s*km", raw_km, re.IGNORECASE) if raw_km else None
        km_limit = km_match.group() if km_match else None

        extra_km = safe_select(f"p[data-testid='additional-kms-{contract}']")

        contract_list.append({
            "contract": contract,
            "base_price": extract_numeric(price),
            "mileage": "Unlimited" if unlimited else km_limit,
            "mileage_note": "" if unlimited else extra_km
        })

    # Common info
    description = safe_select("h6[data-testid='listing-sub-heading']")
    sub_description = safe_select("p[data-testid='description']")
    posted_on = safe_select("p[data-testid='posted-on']")
    dealer_name = safe_select("p[data-testid='name']")
    dealer_type = safe_select("p[data-testid='type']")
    min_driver_age = safe_select("[data-ui-id='details-value-minimum_driver_age']")
    deposit = extract_numeric(safe_select("[data-ui-id='details-value-security_deposit']"))
    refund_period = safe_select("[data-ui-id='details-value-security_refund_period']")
    location = safe_select("div[data-testid='listing-location-map']")

    # Final result
    for contract_entry in contract_list:
        enriched_data.append({
            "sub-url": url,
            "description": description,
            "sub_description": sub_description,
            "posted_on": posted_on,
            "dealer_name": dealer_name,
            "dealer_type": dealer_type,
            "dealer_page": dealer_url,
            **contract_entry,
            "minimum_driver_age": min_driver_age,
            "deposit": deposit,
            "refund_period": refund_period,
            "location": location
        })

    return enriched_data

# ========== PARALLEL SCRAPE WRAPPER ==========

def scrape_main_for_make_model(make, model, logger):
    context = f"{make.upper()}-{model.upper()}"
    logger.set_context(context)
    logger.log(f"Started scraping for Make: {make}, Model: {model}", 0, "🚘")

    url = f"https://dubai.dubizzle.com/motors/rental-cars/{make}/{model}"
    
    driver = make_fast_firefox(headless=True)
    local_main_dataframes, local_detail_dicts = [], []

    try:
        df = scrape_dubizzle_car_data(driver, url)
        logger.log(f"[INFO] Loaded {len(df)} cars from {make}{model}: {url}")

        # Drop the unwanted urls based on make, model and year as defined in Config
        merge_cols = ["make", "model", "year"]
        df_norm = df.copy()
        
        missing_cols = [col for col in merge_cols if col not in df_norm.columns]
        if missing_cols:
            logger.log(f"❌ Skipping {make}-{model} — missing columns: {missing_cols}")
            return [], [], logger.get_logs()
        
        # Normalize for join
        for col in merge_cols:
            df_norm[col] = df_norm[col].astype(str).str.upper()
        config_norm = config.rename(columns={"dubizzle_model": "model"}).copy()
        for col in merge_cols:
            config_norm[col] = config_norm[col].astype(str).str.upper()
        
        filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")
        filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
        
        if filtered_df.empty:
            logger.log(f"⚠️ No {make} {model} results found, inline with Config.")
            return [], [], logger.get_logs()

        local_main_dataframes.append(filtered_df)
        logger.log(f"   🔢 Filtered {make}, {model} cars: {len(filtered_df)}")

        # Detail scraping
        for _, car_row in filtered_df.iterrows():
            sub_url = car_row["sub-url"]
            logger.log(f"      ↪️ Sub-page: {sub_url}")
            try:
                detail_df = scrape_dubizzle_detail(driver, sub_url)
                local_detail_dicts.extend(detail_df)
            except Exception as e:
                logger.log(f"❌ Error scraping sub-page: {e}")
                continue

    except Exception as e:
        logger.log(f"❌ Error scraping main page {make}-{model}: {e}")

    finally:
        driver.quit()
        logger.log("🛑 Browser closed.")

    return local_main_dataframes, local_detail_dicts, logger.get_logs()


# ========== MAIN RUN WITH THREADING ==========

if __name__ == "__main__":
    print("🚀 Launching parallel scraping...")

    main_dataframes, detail_dicts, all_logs = [], [], []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        unique_config = config.drop_duplicates(subset=['make', 'dubizzle_model'])
        for _, row in unique_config.iterrows():
            make = row['make']
            model = row['dubizzle_model']
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
    
    # Title
    mg_models = {'mg3': '3', 'mg5': '5'}

    # Cleaned model column
    model_cleaned = main_df['model'].str.lower().replace(mg_models)

    # Construct title
    main_df['title'] = (
        main_df['make'].str.lower() + ' ' +
        model_cleaned + ' ' +
        main_df['year'].astype(str)
    )

    # Check if the 'base_price' column exists in the DataFrame
    if 'base_price' in detail_df.columns:
        detail_df['savings'] = 0
        detail_df['offered_price'] = detail_df['base_price']
    else:
        print("❌ 'base_price' column not found in final_df")

    # Merge
    final_df = pd.merge(main_df, detail_df, on="sub-url", how="left")
    
    # Apply the categorical types
    final_df["contract"] = final_df["contract"].astype(contract_order)

    # Sort the DataFrame correctly
    df_sorted = final_df.sort_values(by=["contract", "sub-url"]).reset_index(drop=True)
    
    # Reorder columns    
    dubizzle_df = df_sorted[["sub-url", "title", "make", "model", "year", "is_featured", "variant",
        "contract", "base_price", "savings", "offered_price", "description", "sub_description",
        "posted_on", "dealer_name", "dealer_type", "dealer_page", "mileage", "mileage_note",
        "minimum_driver_age", "deposit", "refund_period", "location"]]
    
    # Export as Excel    
    dubizzle_df.to_excel(filename, index=False)
    print(f"\n📁 Saved: {filename}")
    print("\n✅ All scraping complete.")