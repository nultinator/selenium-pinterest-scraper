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



def get_scrapeops_url(url, location="us"):
    payload = {
        "api_key": API_KEY,
        "url": url,
        "country": location,
        "wait": 2000
        }
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    print(proxy_url)
    return proxy_url


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dataclass
class SearchData:
    name: str = ""
    url: str = ""
    image: str = ""

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())

@dataclass
class PinData:
    name: str = ""
    website: str = ""
    stars: int = 0
    follower_count: str = ""
    image: str = ""


    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()



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
            scrapeops_proxy_url = get_scrapeops_url(url, location=location)
            driver.get(scrapeops_proxy_url)
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

                    search_data = SearchData(
                        name=title,
                        url=url,
                        image=img_url
                    )
                    data_pipeline.add_data(search_data)

                

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def process_pin(row, location, retries=3):
    url = row["url"]
    tries = 0
    success = False

    while tries <= retries and not success:

        driver = webdriver.Chrome(options=OPTIONS)
        driver.get(url)

        try:
            main_card = driver.find_element(By.CSS_SELECTOR, "div[data-test-id='CloseupDetails']")
            pin_pipeline = DataPipeline(csv_filename=f"{row['name'][0:20].replace(' ', '-')}.csv")
            website = "n/a"

            website_holder = main_card.find_elements(By.CSS_SELECTOR, "span[style='text-decoration: underline;']")
            has_website = len(website_holder) > 0
            if has_website:
                website = f"https://{website_holder[0].text}"

            star_divs = main_card.find_elements(By.CSS_SELECTOR, "div[data-test-id='rating-star-full']")
            stars = len(star_divs)

            profile_info = main_card.find_element(By.CSS_SELECTOR, "div[data-test-id='follower-count']")

            account_name_div = profile_info.find_element(By.CSS_SELECTOR, "div[data-test-id='creator-profile-name']")
            nested_divs = account_name_div.find_elements(By.CSS_SELECTOR, "div")
            account_name = nested_divs[0].get_attribute("title")
            follower_count = profile_info.text.replace(account_name, "").replace(" followers", "")

            img = "n/a"
            img_container = driver.find_elements(By.CSS_SELECTOR, "div[data-test-id='pin-closeup-image']")
            if len(img_container) > 0:
                img = img_container[0].find_element(By.CSS_SELECTOR, "img").get_attribute("src")

            pin_data = PinData(
                name=account_name,
                website=website,
                stars=stars,
                follower_count=follower_count,
                image=img
            )
            
            pin_pipeline.add_data(pin_data)                    
            pin_pipeline.close_pipeline()


            success = True

        except Exception as e:
            logger.error(f"Exception thrown: {e}")
            logger.warning(f"Failed to process page: {row['url']}")
            logger.warning(f"Retries left: {retries-tries}")
            tries += 1

        finally:
            driver.quit()
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")
    else:
        logger.info(f"Successfully parsed: {row['url']}")




def process_results(csv_file, location, max_threads=5, retries=3):
    logger.info(f"processing {csv_file}")
    with open(csv_file, newline="") as file:
        reader = list(csv.DictReader(file))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(
                process_pin,
                reader,
                [location] * len(reader),
                [retries] * len(reader)
            )

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

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        scrape_search_results(keyword, LOCATION, data_pipeline=crawl_pipeline, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")

    for file in aggregate_files:
        process_results(file, LOCATION, max_threads=MAX_THREADS, retries=MAX_RETRIES) 