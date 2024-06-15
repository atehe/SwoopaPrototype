import requests
import json
import pandas as pd
from datetime import datetime
import os
import argparse
from parsel import Selector


DATA_FOLDER = "./data/kijiji"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

os.makedirs(DATA_FOLDER, exist_ok=True)


URL = "https://api.ca-kijiji-production.classifiedscloud.io/v2/listings"
# ?limit=40&offset=0&topAdCount=6&eaTopAdPosition=1&autoRefine=false&address=Vancouver%2C+BC&attribute=caryear__1980%2C2024&attribute=carmileageinkms__1000%2C1000001&category=174&keywords=toyota&latitude=49.28272914832938&longitude=-123.12073733657598&order=DESC&radius=50000&sort=DATE&type=OFFER"


class KijijiMobile:
    def __init__(
        self,
        query: str,
        lat: str,
        long: str,
        min_price: str = None,
        max_price: str = None,
        min_year: str = "",
        max_year: str = "",
        min_mileage: str = "",
        max_mileage: str = "",
        limit: str = 40,
        car_search=False,
        distance=1000000,  # in meteres
    ):
        self.query = query
        self.lat = lat
        self.long = long
        self.min_price = min_price
        self.max_price = max_price
        self.min_year = min_year
        self.max_year = max_year
        self.min_mileage = min_mileage
        self.max_mileage = max_mileage
        self.car_search = car_search

        self.params = {
            "limit": limit,
            "offset": 0,
            "keywords": query,
            "latitude": self.lat,
            "longitude": self.long,
            "order": "DESC",
            "radius": distance,
            "sort": "DATE",
            "type": "OFFER",
        }

        if self.min_price:
            self.params["minPrice"] = int(self.min_price) * 100

        if self.max_price:
            self.params["maxPrice"] = int(self.max_price) * 100

        if car_search:
            self.params["attribute"] = []

            category = 174
            car_year = f"caryear__{min_year},{max_year}"
            car_mileage = f"carmileageinkms__{min_mileage},{max_mileage}"

            self.params["category"] = category
            if self.min_mileage or self.max_mileage:
                self.params["attribute"].append(car_mileage)

            if self.max_year or self.max_year:
                self.params["attribute"].append(car_year)

        print(self.params)

    def new_listings_filename(self):
        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def get_listings(self):
        response = requests.get(URL, params=self.params)
        self.time_checked = datetime.now().timestamp()

        listing_json = response.json()

        print(response.url)

        with open("resp.json", "w") as f:
            json.dump(listing_json, f, indent=4)

        listings_df = self.parse_listing(listing_json)

        return listings_df

    def check_new_listings(self):
        latest_listings_df = self.get_listings()
        if os.path.exists(LISTINGS_DB):
            old_listings_df = pd.read_csv(LISTINGS_DB)

            latest_listings_df["long"] = latest_listings_df["long"].astype(float)
            latest_listings_df["lat"] = latest_listings_df["lat"].astype(float)

            old_listings_df["long"] = old_listings_df["long"].astype(float)
            old_listings_df["lat"] = old_listings_df["lat"].astype(float)

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

    def parse_listing(self, resp_json: dict):
        results = resp_json.get("results")

        listing_data = []

        for record in results:
            listing_id = record.get("id")
            title = record.get("title")
            location = record.get("locationInfo").get("mapAddress")
            image_url = record.get("thumbnailUrl")
            amount = record.get("price").get("amount")
            amount = amount / 100 if amount else amount
            mileageinkm = record.get("attributes", {}).get("carmileageinkms")
            car_year = record.get("attributes", {}).get("caryear")

            # price = f"{amount} {currency}"
            price = f"$ {amount}" if amount else amount
            url = f"https://www.kijiji.ca/v-cellulaire/ville-de-montreal/iphone-15-neuf-new/{listing_id}"  # redirects to actual URL

            poster_info = record.get("posterInfo").get("id")
            time_posted = record.get("activationDate")
            sorting_date = record.get("sortingDate")

            listing_data.append(
                {
                    "query": self.query,
                    "lat": self.lat,
                    "long": self.long,
                    "listing_id": listing_id,
                    "title": title,
                    "location": location,
                    "image_url": image_url,
                    "price": price,
                    "url": url,
                    "mileageinkm": mileageinkm,
                    "car_year": car_year,
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
        "-la", "--lat", help="Latitude of search location", type=str, default="45.2731"
    )
    parser.add_argument(
        "-lo",
        "--long",
        help="Longitude of search location",
        type=str,
        default="-73.1214",
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
        default=1000,
    )
    parser.add_argument(
        "-max_mileage",
        "--max_mileage",
        help="maximum mileage of result",
        type=float,
        default=1000000,
    )
    parser.add_argument(
        "-car_search",
        "--car_search",
        help="minimum mileage of result",
        type=bool,
        default=True,
    )

    args = parser.parse_args()

    user_search = KijijiMobile(
        args.query,
        str(args.lat),
        str(args.long),
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
