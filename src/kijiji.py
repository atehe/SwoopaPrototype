import requests
import json
import pandas as pd
from datetime import datetime
import os
import argparse
from kijiji_helper import HEADERS, get_location_id, get_seo_url
from parsel import Selector


DATA_FOLDER = "./data/kijiji"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

os.makedirs(DATA_FOLDER, exist_ok=True)


class Kijiji:
    def __init__(
        self,
        query: str,
        city: str,
        state: str,
        min_price: str = None,
        max_price: str = None,
        min_year: str = None,
        max_year: str = None,
        min_mileage: str = None,
        max_mileage: str = None,
        limit: str = 40,
        car_search=False,
    ):
        self.query = query
        self.city = city
        self.state = state
        self.min_price = min_price
        self.max_price = max_price
        self.min_year = min_year
        self.max_year = max_year
        self.min_mileage = min_mileage
        self.max_mileage = max_mileage
        location_id = get_location_id(f"{self.city}, {self.state}")
        self.car_search = car_search

        self.url = get_seo_url(
            query=self.query,
            location_id=location_id,
            min_price=self.min_price,
            max_price=self.max_price,
            min_year=self.min_year,
            max_year=self.max_year,
            min_mileage=self.min_mileage,
            max_mileage=self.max_mileage,
            car_search=self.car_search,
            limit=limit,
        )
        print(self.url)

    def new_listings_filename(self):
        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def get_listings(self):
        response = requests.get(self.url, headers=HEADERS)
        self.time_checked = datetime.now().timestamp()

        listing_page = Selector(response.text)

        listing_json = listing_page.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        listing_json = json.loads(str(listing_json))

        listings_df = self.parse_listing(listing_json)
        return listings_df

    def check_new_listings(self):
        latest_listings_df = self.get_listings()
        if os.path.exists(LISTINGS_DB):
            old_listings_df = pd.read_csv(LISTINGS_DB)

            latest_listings_df["state"] = latest_listings_df["state"].astype(float)
            latest_listings_df["city"] = latest_listings_df["city"].astype(float)

            old_listings_df["state"] = old_listings_df["state"].astype(float)
            old_listings_df["city"] = old_listings_df["city"].astype(float)

            merge_df = latest_listings_df.merge(
                old_listings_df[["listing_id", "query", "state", "city"]],
                on=["listing_id", "query", "state", "city"],
                how="left",
                indicator=True,
            )
            new_listings_df = merge_df[merge_df["_merge"] == "left_only"]

        else:
            new_listings_df = latest_listings_df

        ## Notify of new listings
        print(
            f"Found {len(new_listings_df)} new listings for {self.query} in city {self.city} and state {self.state}"
        )
        new_listings_df.to_csv(self.new_listings_filename(), index=None)

        ## Add new listing df to the listing db so notification doesnt come up again
        if "_merge" in new_listings_df.columns:
            new_listings_df = new_listings_df.drop(columns=["_merge"])

        new_listings_df.to_csv(
            LISTINGS_DB, mode="a", index=None, header=not os.path.exists(LISTINGS_DB)
        )

    def parse_listing(self, resp_json: dict):
        props = resp_json.get("props")
        page_props = props.get("pageProps")
        appollo_state = page_props.get("__APOLLO_STATE__")

        listing_data = []

        for record, value in appollo_state.items():
            if not record.startswith("ListingV2"):
                continue

            listing_id = value.get("id")
            title = value.get("title")
            location = value.get("location").get("address")
            image_url = value.get("imageUrls")[0] if value.get("imageUrls") else None

            amount = value.get("price").get("amount")
            amount = amount / 100

            currency = value.get("price").get("currency")

            attributes = value.get("attributes")
            for attr in attributes:
                if attr.get("name") == "carmileageinkms":
                    mileageinkm = attr.get("values")[0] if attr.get("values") else None
                    break
            else:
                mileageinkm = None

            price = f"{amount} {currency}"
            url = value.get("seoUrl")
            if not url.startswith("http"):
                url = f"https://www.kijiji.ca/{url.strip('/')}"

            poster_info = value.get("posterInfo").get("__ref")
            time_posted = value.get("activationDate")
            sorting_date = value.get("sortingDate")

            listing_data.append(
                {
                    "query": self.query,
                    "city": self.city,
                    "state": self.state,
                    "listing_id": listing_id,
                    "title": title,
                    "location": location,
                    "image_url": image_url,
                    "price": price,
                    "url": url,
                    "mileageinkm": mileageinkm,
                    "poster_info": poster_info,
                    "time_posted": time_posted,
                    "sorted_time": sorting_date,
                    "time_found": self.time_checked,
                }
            )

        print(len(listing_data))

        listing_df = pd.DataFrame(listing_data)
        return listing_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", help="Search Query", type=str, required=True)
    parser.add_argument(
        "-city", "--city", help="City of search location", type=str, default="Alberta"
    )
    parser.add_argument(
        "-state",
        "--state",
        help="State of search location",
        type=str,
        # default="Saskatchewan",
        default="",
    )
    parser.add_argument(
        "-limit", "--limit", help="Number of resutl returned", type=int, default=40
    )
    parser.add_argument(
        "-min", "--min_price", help="Minimum Price of result", type=float, default=20000
    )
    parser.add_argument(
        "-max", "--max_price", help="Maximum Price of result", type=float, default=25000
    )
    parser.add_argument(
        "-min_year", "--min_year", help="Minimum Year of result", type=int, default=1990
    )
    parser.add_argument(
        "-max_year",
        "--max_year",
        help="maximum Year of result",
        type=int,
        default=2024,
    )
    parser.add_argument(
        "-min_mileage",
        "--min_mileage",
        help="minimum mileage of result",
        type=float,
        default=100,
    )
    parser.add_argument(
        "-max_mileage",
        "--max_mileage",
        help="maximum mileage of result",
        type=float,
        default=100000,
    )
    parser.add_argument(
        "-car_search",
        "--car_search",
        help="minimum mileage of result",
        type=bool,
        default=True,
    )

    args = parser.parse_args()

    user_search = Kijiji(
        args.query,
        str(args.city),
        str(args.state),
        min_price=args.min_price,
        max_price=args.max_price,
        min_year=args.min_year,
        max_year=args.max_year,
        min_mileage=args.min_mileage,
        max_mileage=args.max_mileage,
        limit=args.limit,
        car_search=args.car_search,
    )
    user_search.check_new_listings()
# "Regina, Saskatchewan"
