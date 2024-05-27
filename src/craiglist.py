import pandas as pd
from datetime import datetime
import os
import requests
import argparse
from craiglist_categories import CATEGORIES
import json


DATA_FOLDER = "./data/craiglist"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

API_URL = "https://sapi.craigslist.org/web/v8/postings/search/full"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

os.makedirs(DATA_FOLDER, exist_ok=True)


def get_item_attribute(id, item_list):
    for rec in item_list:
        if isinstance(rec, list) and len(rec) > 1 and rec[0] == id:
            attribute = rec
            break
    else:
        return [None]

    return attribute[1:]


class Craiglist:

    def __init__(
        self,
        query: str,
        lat: float,
        long: float,
        min_price: str = None,
        max_price: str = None,
        min_mileage: str = None,
        max_mileage: str = None,
        distance: str = "60",
        sort_by: str = "date",
    ):
        self.query = query
        self.lat = lat
        self.long = long
        self.distance = distance
        self.min_price = min_price
        self.max_price = max_price
        self.min_mileage = min_mileage
        self.max_mileage = max_mileage

        self.params = {
            "lat": self.lat,
            "lon": self.long,
            "search_distance": self.distance,
            "query": self.query,
            "sort": sort_by,
            "cc": "US",
            "batch": "39-0-360-1-0",
            "lang": "en",
            "searchPath": "sss",
        }

        if self.min_price:
            self.params["min_price"] = min_price

        if self.max_price:
            self.params["max_price"] = max_price

        if self.min_mileage:
            self.params["min_auto_year"] = min_mileage

        if self.max_mileage:
            self.params["max_auto_year"] = max_mileage

    def new_listings_filename(self):

        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def get_listings(self):
        response = requests.get(
            API_URL,
            headers=HEADERS,
            params=self.params,
        )
        self.time_checked = datetime.now().timestamp()

        listings_df = self.parse_listing(response.json())
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

    def parse_listing(self, resp):

        data = resp.get("data")

        source_url = data.get("canonicalUrl", "").strip("/")

        decode = data.get("decode") or {}
        locations = decode.get("locations")

        formatted_locs = []
        for location in locations:

            try:

                subdomain = location[1] if len(location) > 1 else None
                subregion = location[2] if len(location) > 2 else None

                formatted_locs.append(
                    {
                        "subdomain": subdomain,
                        "subregion": subregion,
                    }
                )
            except TypeError:
                formatted_locs.append({})

        min_post_id = decode.get("minPostingId")
        min_post_date = decode.get("minPostedDate")

        listing_data = []

        items = data.get("items")
        for item in items:

            listing_id = item[0] + min_post_id
            time_posted = item[1] + min_post_date
            category = CATEGORIES.get(item[2])

            location = item[4]

            index, lat, long = location.split("~")
            index = index.split(":")[0]

            subregion = formatted_locs[int(index)].get("subregion")
            subdomain = formatted_locs[int(index)].get("subdomain")

            images = get_item_attribute(4, item)

            images = [
                f"https://images.craigslist.org/{image.split(':')[-1 ]}_600x450.jpg"
                for image in images
                if isinstance(image, str)
            ]

            price = get_item_attribute(10, item)[0]
            handle = get_item_attribute(6, item)[0]

            title = item[-1]

            if subregion:
                url = f"https://{subdomain}.craigslist.org/{subregion}/{category}/d/{handle}/{listing_id}.html"
            else:
                url = f"https://{subdomain}.craigslist.org/{category}/d/{handle}/{listing_id}.html"

            listing_data.append(
                {
                    "query": self.query,
                    "lat": self.lat,
                    "long": self.long,
                    "listing_id": listing_id,
                    "title": title,
                    "location": f"{lat}, {long}",
                    "image_url": images[0] if images else None,
                    "price": price,
                    "url": url,
                    "time_posted": time_posted,
                    "time_found": self.time_checked,
                    "source_url": source_url,
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
    parser.add_argument(
        "-min", "--min_price", help="Minimum Price of result", type=str, default=None
    )
    parser.add_argument(
        "-max", "--max_price", help="Maximum Price of result", type=str, default=None
    )
    parser.add_argument(
        "-min_mileage",
        "--min_mileage",
        help="Minimum Mileage of Vehicle",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-max_mileage",
        "--max_mileage",
        help="Maximum Mileage of Vehicle",
        type=str,
        default=None,
    )
    args = parser.parse_args()

    user_search = Craiglist(
        args.query,
        args.lat,
        args.long,
        min_price=args.min_price,
        max_price=args.max_price,
        min_mileage=args.min_mileage,
        max_mileage=args.max_mileage,
        distance=args.dist,
    )
    user_search.check_new_listings()
