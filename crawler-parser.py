import os
import csv
import json
import logging
from urllib.parse import urlencode
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from dataclasses import dataclass, field, fields, asdict
from time import sleep

OPTIONS = webdriver.ChromeOptions()

prefs = {
    "profile.managed_default_content_settings.javascript": 2
}
OPTIONS.add_experimental_option("prefs", prefs)

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
OPTIONS.add_argument(f"useragent={user_agent}")

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    tries = 0
    success = False
    
    while tries <= retries and not success:
        url = f"https://www.pinterest.com/search/pins/?q={formatted_keyword}&rs=typed"
        driver = webdriver.Chrome(options=OPTIONS)
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(10)
        try:
            driver.get(url)
            logger.info(f"Fetched {url}")
                
            ## Extract Data            
            div_cards = driver.find_elements(By.CSS_SELECTOR, "div")

            print("found div cards:", len(div_cards))
            

            for div_card in div_cards:
                is_card = div_card.get_attribute("data-grid-item")
                if is_card:
                    a_element = div_card.find_element(By.CSS_SELECTOR, "a")
                    title = a_element.get_attribute("aria-label")
                    href = a_element.get_attribute("href").replace("https://proxy.scrapeops.io", "")
                    url = f"https://pinterest.com{href}"
                    img = div_card.find_element(By.CSS_SELECTOR, "img")
                    img_url = img.get_attribute("src")

                    search_data = {
                        "name": title,
                        "url": url,
                        "image": img_url
                    }
                    
                    print(search_data)             

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["grilling"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")