# -*- coding: utf-8 -*-
"""
Dubizzle Car Rental Scraper
Enhanced version with better error handling and retry mechanisms
Created on Mon Jun 24 15:36:06 2025
Last Modified on Sat Jul 12 20:37:47 2025
@author: Jatin Bhardwaj
"""

from pathlib import Path
import pandas as pd
import re
import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
from pandas.api.types import CategoricalDtype
from playwright.async_api import async_playwright
import random
import logging
import sys

# Ensure logs directory exists
logs_dir = Path.cwd() / "logs"
logs_dir.mkdir(exist_ok=True)

# Configure logging
log_filename = logs_dir / "dubizzle_scraper.log"

def configure_logging():
    if log_filename.exists():
        try:
            log_filename.unlink()  # Delete the existing log file
            print(f"🗑️  Deleted previous log file: {log_filename}")
        except Exception as e:
            print(f"⚠️  Could not delete previous log file: {e}")

    logger = logging.getLogger("dubizzle")
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, [file_handler, console_handler]

# Call the logging configuration at module level
logger, log_handlers = configure_logging()

# Configuration
CONFIG = {
    "timeout": 60000,  # 60 seconds
    "max_retries": 3,
    "retry_delay": 5,
    "semaphore_limit": 5,
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ],
    "wait_conditions": ["domcontentloaded", "load", "networkidle"]
}

class ScraperLogger:
    def __init__(self, context):
        self.context = context
        self.counter = 0
        self.buffer = []
        self.logger = logging.getLogger("dubizzle")

    def log(self, message, level="info"):
        self.counter += 1
        tag = f"[{self.context}.{self.counter}]"
        line = f"{tag} {message.strip()}"
        self.buffer.append((level.lower(), line))

    def flush(self):
        header = f"========== {self.context} =========="
        self.logger.info(header)
        for level, line in self.buffer:
            if level == "info":
                self.logger.info(line)
            elif level == "warning":
                self.logger.warning(line)
            elif level == "error":
                self.logger.error(line)
            else:
                self.logger.debug(line)
        self.logger.info("")

def extract_numeric(text):
    if not text or pd.isna(text):
        return None
    nums = ''.join(filter(str.isdigit, text))
    return int(nums) if nums else None

def fix_spacing(text):
    if pd.isna(text): 
        return text
    text = re.sub(r"AED(\d+)", r"AED \1", text)
    text = re.sub(r"(?<=\d)(?=km)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\d)(?=for)", " ", text) 
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
    context = await browser.new_context(
        user_agent=random.choice(CONFIG["user_agents"]),
        viewport={"width": 1366, "height": 768}
    )
    return browser, context

async def scroll_to_bottom_async(page, pause=2, max_attempts=3):
    for _ in range(max_attempts):
        prev_height = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            break

async def wait_until_listing_card_populated(page, max_tries=10, delay=1.5):
    for attempt in range(max_tries):
        content_html = await page.locator("#listing-card-wrapper").inner_html()
        if "data-testid" in content_html:
            return True
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(delay)
    return False

async def scrape_dubizzle_car_data_async(page, url, logger):
    for attempt in range(CONFIG["max_retries"]):
        try:
            wait_condition = random.choice(CONFIG["wait_conditions"])
            logger.log(f"Attempt {attempt + 1}: Loading page (wait until: {wait_condition})")
            
            await page.goto(url, wait_until=wait_condition, timeout=CONFIG["timeout"])
            
            if not await wait_until_listing_card_populated(page):
                raise Exception("Listing cards not populated after waiting")
                
            await scroll_to_bottom_async(page)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            car_cards = soup.select("#listing-card-wrapper a[data-testid^='listing-']")
            
            if not car_cards:
                raise Exception("No car cards found in the page")
                
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
                except Exception as e:
                    logger.log(f"Error processing card: {str(e)}", "warning")
                    continue
            
            logger.log(f"Successfully scraped {len(data)} listings")
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.log(f"Attempt {attempt + 1} failed: {str(e)}", "warning")
            if attempt == CONFIG["max_retries"] - 1:
                raise
            await asyncio.sleep(CONFIG["retry_delay"])
    
    return pd.DataFrame()

async def wait_until_detail_card_populated(page, max_tries=10, delay=1.5):
    for attempt in range(max_tries):
        try:
            content_html = await page.locator("body").inner_html()
            if ("data-testid=\"listing-sub-heading\"" in content_html or 
                "data-testid=\"rental-price-" in content_html):
                return True
        except:
            pass
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(delay)
    return False

async def scrape_dubizzle_detail_async(page, url, logger):
    logger.log(f"Starting scrape for {url}")
    for attempt in range(CONFIG["max_retries"]):
        try:
            wait_condition = random.choice(CONFIG["wait_conditions"])
            logger.log(f"Attempt {attempt + 1}: Loading detail page (wait until: {wait_condition})")
            
            await page.goto(url, wait_until=wait_condition, timeout=CONFIG["timeout"])
            
            if not await wait_until_detail_card_populated(page):
                raise Exception("Detail content not populated after waiting")
                
            await scroll_to_bottom_async(page)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            enriched = []
            
            def safe_select(selector, attr="text", many=False):
                try:
                    el = soup.select(selector)
                    if not el:
                        return "" if not many else []
                    if attr == "text":
                        return [e.get_text(strip=True) for e in el] if many else el[0].get_text(strip=True)
                    return [e.get(attr, "") for e in el] if many else el[0].get(attr, "")
                except:
                    return "" if not many else []
            
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
            
            if not contract_list:
                raise Exception("No contract information found")
            
            description = safe_select("h6[data-testid='listing-sub-heading']")
            sub_description = safe_select("p[data-testid='description']")
            posted_on = safe_select("p[data-testid='posted-on']")
            dealer_name = safe_select("p[data-testid='name']")
            dealer_type = safe_select("p[data-testid='type']")
            min_age = safe_select("[data-ui-id='details-value-minimum_driver_age']")
            deposit = extract_numeric(safe_select("[data-ui-id='details-value-security_deposit']"))
            refund = safe_select("[data-ui-id='details-value-security_refund_period']")
            loc = safe_select("div[data-testid='listing-location-map']")
            
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
            
            logger.log(f"Successfully scraped {len(enriched)} contract options")
            return enriched
            
        except Exception as e:
            logger.log(f"Attempt {attempt + 1} failed: {str(e)}", "warning")
            if attempt == CONFIG["max_retries"] - 1:
                raise
            await asyncio.sleep(CONFIG["retry_delay"])
    
    return []

async def main():
    try:
        logger.info("🚀 Starting Dubizzle Scraper")
        # Load configuration
        config_path = Path.cwd() / "config/make_model.csv"
        config = pd.read_csv(config_path, usecols=["make", "year", "dubizzle_model"], low_memory=False)
        config = config[config["dubizzle_model"].notna() & (config["dubizzle_model"].str.strip() != "")]
        filename = Path.cwd() / f"output/dubizzle_rentals.xlsx"
        
        # Initialize browser
        browser, context = await make_fast_firefox_async()
        semaphore = asyncio.Semaphore(CONFIG["semaphore_limit"])
        
        # Prepare data collection
        main_dataframes = []
        detail_dicts = []
        
        # Process each make/model combination
        unique_config = config.drop_duplicates(subset=["make", "dubizzle_model"])
        
        async def scrape_task(make, model):
            logger = ScraperLogger(f"{make.upper()}-{model.upper()}")
            await semaphore.acquire()
            try:
                page = await context.new_page()
                list_url = f"https://dubai.dubizzle.com/motors/rental-cars/{make}/{model}"
                logger.log(f"Starting scrape for {list_url}")
                
                try:
                    # Scrape listing page
                    df_main = await scrape_dubizzle_car_data_async(page, list_url, logger)
                    
                    if df_main.empty:
                        logger.log("No listings found", "warning")
                        return [], []
                    
                    # Filter results based on configuration
                    merge_cols = ["make", "model", "year"]
                    df_norm = df_main.copy()
                    config_norm = config.rename(columns={"dubizzle_model": "model"}).copy()
                    config_norm['model'] = config_norm['model'].apply(lambda x: x.replace('-', ' ') if '-' in x else x)
                    
                    for col in merge_cols:
                        df_norm[col] = df_norm[col].astype(str).str.upper()
                        config_norm[col] = config_norm[col].astype(str).str.upper()
                    
                    filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")
                    filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
                    
                    logger.log(f"Filtered to {len(filtered_df)} matching listings")
                    
                    if filtered_df.empty:
                        return [], []
                    
                    # Scrape detail pages
                    enriched = []
                    for _, row in filtered_df.iterrows():
                        try:
                            detail = await scrape_dubizzle_detail_async(page, row["sub-url"], logger)
                            enriched.extend(detail)
                        except Exception as e:
                            logger.log(f"Detail page error: {str(e)}", "error")
                            continue
                    
                    return [filtered_df], enriched
                
                except Exception as e:
                    logger.log(f"Scraping failed: {str(e)}", "error")
                    return [], []
                
            finally:
                await page.close()
                semaphore.release()
                logger.flush()
        
        # Run all tasks
        tasks = [scrape_task(row["make"], row["dubizzle_model"]) for _, row in unique_config.iterrows()]
        results = await asyncio.gather(*tasks)
        
        # Combine results
        for main_df, detail in results:
            main_dataframes.extend(main_df)
            detail_dicts.extend(detail)
        
        # Save results
        if not main_dataframes:
            logger.error("❌ No data found. Check the logs for errors.")
            return
        
        main_df = pd.concat(main_dataframes, ignore_index=True)
        detail_df = pd.DataFrame(detail_dicts)
        
        # Clean and merge data
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
        
        contract_order = CategoricalDtype(["daily", "weekly", "monthly"], ordered=True)
        final_df = pd.merge(main_df, detail_df, on="sub-url", how="left")
        final_df["contract"] = final_df["contract"].astype(contract_order)
        df_sorted = final_df.sort_values(by=["contract", "sub-url"]).reset_index(drop=True)
        
        # Select and save columns
        output_cols = [
            "sub-url", "title", "make", "model", "year", "is_featured", "variant",
            "contract", "base_price", "savings", "offered_price", "description", "sub_description",
            "posted_on", "dealer_name", "dealer_type", "dealer_page", "mileage", "mileage_note",
            "minimum_driver_age", "deposit", "refund_period", "location"
        ]
        
        output_df = df_sorted[[col for col in output_cols if col in df_sorted.columns]]
        output_df.to_excel(filename, index=False)
        
        logger.info(f"✅ Successfully saved data to {filename}")
        logger.info(f"📄 Logs saved to {log_filename}")
        
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {str(e)}", exc_info=True)
    finally:
        try:
            await context.close()
            await browser.close()
            logging.shutdown()
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")
            
        for handler in log_handlers:
            handler.flush()
            handler.close()
            logging.getLogger("dubizzle").removeHandler(handler)

if __name__ == "__main__":
    try:
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.run(main())
    except Exception as e:
        logger.error(f"❌ Script crashed: {str(e)}", exc_info=True)
        logging.shutdown()