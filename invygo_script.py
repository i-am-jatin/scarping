# -*- coding: utf-8 -*-
"""
Invygo Car Rental Scraper
Enhanced version with better error handling and retry mechanisms
Created on Mon Jun 24 10:20:13 2025
Last Modified on Sat Jul 12 21:15:10 2025
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
from urllib.parse import unquote

# Ensure logs directory exists
logs_dir = Path.cwd() / "logs"
logs_dir.mkdir(exist_ok=True)

# Configure logging
log_filename = logs_dir / "invygo_scraper.log"

def configure_logging():
    if log_filename.exists():
        try:
            log_filename.unlink()  # Delete the existing log file
            print(f"🗑️  Deleted previous log file: {log_filename}")
        except Exception as e:
            print(f"⚠️  Could not delete previous log file: {e}")

    logger = logging.getLogger("invygo")
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
    "wait_conditions": ["domcontentloaded", "load", "networkidle"],
    "modes": ["weekly", "monthly"]
}

class ScraperLogger:
    def __init__(self, context):
        self.context = context
        self.counter = 0
        self.buffer = []
        self.logger = logging.getLogger("invygo")

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
        try:
            content_html = await page.locator("div.grid.grid-cols-1").first.inner_html()
            if "a href=\"/en-ae/dubai/rent-" in content_html:
                return True
        except:
            pass
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(delay)
    return False

async def scrape_invygo_car_data_async(page, url, mode, logger):
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
            car_cards = soup.find_all("a", href=re.compile(f"^/en-ae/dubai/rent-{mode}-"))
            
            if not car_cards:
                raise Exception("No car cards found in the page")
                
            data = []
            for card in car_cards:
                try:
                    info_div = card.find("div", class_="p-4 space-y-2")
                    if not info_div:
                        continue

                    year_tag = info_div.find("p", class_="text-[#667085] text-xs font-medium")
                    year = int(year_tag.text.strip()) if year_tag else None

                    title_tag = info_div.find("h3", class_="text-[#0C111D] font-semibold text-sm")
                    title = title_tag.text.strip() if title_tag else None

                    contract_tags = info_div.find_all("div", class_="text-[#0C111D] font-semibold text-xs")
                    mileage = contract_tags[1].text.strip() if len(contract_tags) > 1 else None

                    promo_tag = card.find("div", class_=lambda x: x and "bg-" in x and ("#EC625B" in x or "EC625B" in x))
                    promotion = "yes" if promo_tag else "no"

                    full_url = f"https://invygo.com{card['href']}"
                    make, model = extract_make_model_from_url(full_url)

                    data.append({
                        "sub-url": full_url,
                        "title": title,
                        "make": make,
                        "model": model,
                        "year": year,
                        "runnings_kms": mileage,
                        "promotion": promotion,
                        "contract": mode
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
            content_html = await page.locator('div.rounded-xl.border-GREY-30').first.inner_html()
            if 'data-testid="booking-contract-length"' in content_html:
                return True
        except:
            pass
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(delay)
    return False

async def scrape_invygo_detail_async(page, url, logger):
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
            
            enriched_data = []
            seen_durations = set()

            duration_elements = await page.query_selector_all('[data-testid="booking-contract-length"] [role="presentation"]')

            for index in range(len(duration_elements)):
                try:
                    current_elements = await page.query_selector_all('[data-testid="booking-contract-length"] [role="presentation"]')
                    elem = current_elements[index]
                    
                    await elem.scroll_into_view_if_needed()
                    await page.evaluate("e => e.click()", elem)
                    
                    try:
                        await page.wait_for_function(
                            """() => {
                                const priceEl = document.querySelector('div.text-black.font-inter.text-3xl');
                                return priceEl && priceEl.textContent.includes('AED');
                            }""",
                            timeout=6000
                        )
                    except:
                        await asyncio.sleep(1)

                    html = await page.content()
                    soup = BeautifulSoup(html, 'html.parser')

                    duration_block = soup.select('[data-testid="booking-contract-length"] [role="presentation"]')[index]
                    duration_text = duration_block.select_one('div.text-cool-gray-900').get_text(strip=True)
                    
                    if duration_text in seen_durations:
                        continue
                    seen_durations.add(duration_text)

                    price_tag = soup.find("div", class_=re.compile(r"text-black.*text-3xl"))
                    price = int(clean_price(price_tag.get_text(strip=True))) if price_tag else None

                    insurance_result = {"standard_cover_insurance": "No additional cost", "full_cover_insurance": None}
                    insurance_blocks = soup.select('[data-testid="booking-insurance-options"] [role="presentation"]')
                    for block in insurance_blocks:
                        title = block.select_one('div.text-cool-gray-900')
                        note = block.select_one('div.text-grey-50')
                        if title:
                            title_text = title.get_text(strip=True).lower()
                            note_text = note.get_text(strip=True) if note else None
                            if "full cover" in title_text:
                                insurance_result["full_cover_insurance"] = note_text

                    mileage_list = []
                    for m in soup.select('[data-testid="booking-milage-options"] [role="presentation"]'):
                        m_text = m.select_one('div.text-cool-gray-900')
                        m_note = m.select_one('div.text-grey-50')
                        if m_text:
                            mileage_list.append({
                                "mileage": m_text.get_text(strip=True),
                                "mileage_note": m_note.get_text(strip=True) if m_note else None,
                                "mileage_numeric": extract_numeric(m_note.get_text(strip=True)) if m_note else 0
                            })

                    for mileage_entry in mileage_list:
                        enriched_data.append({
                            "sub-url": url,
                            "duration": duration_text,
                            "savings": extract_numeric(duration_block.select_one('div.text-grey-50').get_text(strip=True)),
                            "offered_price": price,
                            **mileage_entry,
                            **insurance_result
                        })

                except Exception as e:
                    logger.log(f"Error processing option {index}: {str(e)}", "warning")
                    continue
            
            logger.log(f"Successfully scraped {len(enriched_data)} contract options")
            return enriched_data
            
        except Exception as e:
            logger.log(f"Attempt {attempt + 1} failed: {str(e)}", "warning")
            if attempt == CONFIG["max_retries"] - 1:
                raise
            await asyncio.sleep(CONFIG["retry_delay"])
    
    return []

async def main():
    try:
        logger.info("🚀 Starting Invygo Scraper")
        # Load configuration
        config_path = Path.cwd() / "config/make_model.csv"
        config = pd.read_csv(config_path, usecols=["make", "year", "invygo_model"], low_memory=False)
        config = config[config["invygo_model"].notna() & (config["invygo_model"].str.strip() != "")]
        filename = Path.cwd() / f"output/invygo_rentals.xlsx"
        
        # Initialize browser
        browser, context = await make_fast_firefox_async()
        semaphore = asyncio.Semaphore(CONFIG["semaphore_limit"])
        
        # Prepare data collection
        main_dataframes = []
        detail_dicts = []
        
        # Define categorical types
        contract_order = CategoricalDtype(["weekly", "monthly"], ordered=True)
        duration_order = CategoricalDtype(["1 week", "1 month", "3 months", "6 months", "9 months"], ordered=True)
        
        # Process each rental mode
        async def scrape_mode(mode):
            logger = ScraperLogger(f"{mode.upper()}-LISTINGS")
            await semaphore.acquire()
            try:
                page = await context.new_page()
                list_url = f"https://invygo.com/en-ae/dubai/rent-{mode}-cars"
                logger.log(f"Starting scrape for {list_url}")
                
                try:
                    # Scrape listing page
                    df_main = await scrape_invygo_car_data_async(page, list_url, mode, logger)
                    
                    if df_main.empty:
                        logger.log("No listings found", "warning")
                        return [], []
                    
                    # Filter results based on configuration
                    merge_cols = ["make", "model", "year"]
                    df_norm = df_main.copy()
                    config_norm = config.rename(columns={"invygo_model": "model"}).copy()
                    
                    for col in merge_cols:
                        df_norm[col] = df_norm[col].astype(str).str.upper()
                        config_norm[col] = config_norm[col].astype(str).str.upper()
                    
                    filtered_df = df_norm.merge(config_norm, on=merge_cols, how="inner")
                    filtered_df["year"] = pd.to_numeric(filtered_df["year"], errors="coerce").astype("Int64")
                    filtered_df['title'] = filtered_df['title'].str.lower() + ' ' + filtered_df['year'].astype(str)
                    
                    logger.log(f"Filtered to {len(filtered_df)} matching listings")
                    
                    if filtered_df.empty:
                        return [], []
                    
                    # Scrape detail pages
                    enriched = []
                    for _, row in filtered_df.iterrows():
                        try:
                            detail = await scrape_invygo_detail_async(page, row["sub-url"], logger)
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
        tasks = [scrape_mode(mode) for mode in CONFIG["modes"]]
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
        
        # Process and merge data
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
        
        # Select and save columns
        output_cols = [
            "sub-url", "title", "make", "model", "year", "promotion", "runnings_kms",
            "contract", "base_price", "savings", "offered_price", "duration", "mileage",
            "mileage_note", "standard_cover_insurance", "full_cover_insurance"
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
            logging.getLogger("invygo").removeHandler(handler)

if __name__ == "__main__":
    try:
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.run(main())
    except Exception as e:
        logger.error(f"❌ Script crashed: {str(e)}", exc_info=True)
        logging.shutdown()