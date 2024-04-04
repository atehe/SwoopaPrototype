import pandas as pd
from datetime import datetime
import os

from parsel import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_binary  # noqa: F401
import argparse


DATA_FOLDER = "./data/craiglist"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

os.makedirs(DATA_FOLDER, exist_ok=True)


class Craiglist:

    def __init__(
        self,
        query: str,
        lat: float,
        long: float,
        distance: str = "50",
        sort_by: str = "date",
    ):
        self.query = query
        self.lat = lat
        self.long = long
        self.distance = distance
        self.search_url = f"https://ksu.craigslist.org/search/sss?lat={self.lat}&lon={self.long}&query={self.query}&search_distance={self.distance}&sort={sort_by}"

    def new_listings_filename(self):

        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def create_browser(self):
        options = Options()

        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)
        return driver

    def wait_for(self, xpath, clickable=False, timer=5):
        wait = WebDriverWait(self.driver, timer)

        # try:
        if clickable:
            element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        else:
            element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

        return element
        # except TimeoutException:
        #     print("Element not found")

    def get_listings(self):
        self.driver = self.create_browser()
        self.driver.get(self.search_url)

        self.wait_for('//div[@class="cl-search-results"]//li', timer=15)

        page = Selector(self.driver.page_source)
        self.time_checked = datetime.now().timestamp()

        listings_df = self.parse_listing(page)
        return listings_df

    def check_new_listings(self):
        latest_listings_df = self.get_listings()
        if os.path.exists(LISTINGS_DB):

            old_listings_df = pd.read_csv(LISTINGS_DB)

            latest_listings_df["long"] = latest_listings_df["long"].astype(float)
            latest_listings_df["lat"] = latest_listings_df["lat"].astype(float)
            latest_listings_df["listing_id"] = latest_listings_df["listing_id"].astype(
                int
            )

            old_listings_df["long"] = old_listings_df["long"].astype(float)
            old_listings_df["lat"] = old_listings_df["lat"].astype(float)
            old_listings_df["listing_id"] = old_listings_df["listing_id"].astype(int)

            merge_df = latest_listings_df.merge(
                old_listings_df[["listing_id", "query", "long", "lat"]],
                on=["listing_id", "query", "long", "lat"],
                how="left",
                indicator=True,
            )
            new_listings_df = merge_df[merge_df["_merge"] == "left_only"]

        else:
            new_listings_df = latest_listings_df

        ## Notify of new listings
        print(
            f"Found {len(new_listings_df)} new listings for {self.query} in lat {self.lat} and long {self.long}"
        )
        new_listings_df.to_csv(self.new_listings_filename(), index=None)

        ## Add new listing df to the listing db so notification doesnt come up again
        if "_merge" in new_listings_df.columns:
            new_listings_df = new_listings_df.drop(columns=["_merge"])

        new_listings_df.to_csv(
            LISTINGS_DB, mode="a", index=None, header=not os.path.exists(LISTINGS_DB)
        )
        self.driver.quit()

    def parse_listing(self, page):

        records = page.xpath('//div[@class="cl-search-results"]//li')
        listing_data = []

        for record in records:

            listing_id = record.xpath(".//@data-pid").get()
            # image_url = ", ".join(record.xpath(".//img/@src").getall())
            price = record.xpath('.//span[@class="priceinfo"]//text()').get()
            title = record.xpath(".//@title").get()
            url = record.xpath(".//a/@href").get()
            time_posted = record.xpath('.//div[@class="meta"]/text()[1]').get()
            location = record.xpath('.//div[@class="meta"]/text()[2]').get()

            listing_data.append(
                {
                    "query": self.query,
                    "lat": self.lat,
                    "long": self.long,
                    "listing_id": listing_id,
                    "title": title,
                    # "image_url": image_url,
                    "price": price,
                    "url": url,
                    "location": location,
                    "time_posted": time_posted,
                    "time_found": self.time_checked,
                }
            )
        listing_df = pd.DataFrame(listing_data)
        listing_df.drop_duplicates(inplace=True)
        return listing_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", help="Search Query", type=str, required=True)
    parser.add_argument(
        "-la", "--lat", help="Latitude of search location", type=str, default="40.7128"
    )
    parser.add_argument(
        "-lo",
        "--long",
        help="Longitude of search location",
        type=str,
        default="-74.0060",
    )
    parser.add_argument(
        "-d", "--dist", help="Distance or search radius", type=str, default="50"
    )
    args = parser.parse_args()

    user_search = Craiglist(args.query, args.lat, args.long, args.dist)
    user_search.check_new_listings()
