import pandas as pd
from datetime import datetime
import os
import requests
import argparse
import json
import math


API_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
USER_TOKEN = "v^1.1#i^1#I^3#r^0#f^0#p^1#t^H4sIAAAAAAAAAOVYe2wURRjv9YHWQrEBBRpiyhZJBHdvd++97Z1cX/RCr9f2ruUh2szuzvbW7u1uduZarg3alEAMBJ9BNIACCfEfERMeAXmEiNEY1CAx+IcoBpWARqMQMVE07m5LuVbCq5fYxPvnMt98883v95vvm5kdemBS8fy1jWt/n+K4J3/bAD2Q73AwJXTxpKIFpQX55UV5dJaDY9vA3IHCwYIL1QikFJ1rg0jXVAQrVqYUFXG2MUikDZXTAJIRp4IURBwWuHg42sSxFM3phoY1QVOIikhdkBBoj88FJNEvQreHZiXTql6LmdCChN/n8YiSj5EAL7okl9WPUBpGVISBioMES7NukvaSLJtgXBxLc7SHcgX8y4mKDmggWVNNF4omQjZczh5rZGG9OVSAEDSwGYQIRcIN8Vg4UlffnKh2ZsUKDesQxwCn0ehWrSbCig6gpOHNp0G2NxdPCwJEiHCGhmYYHZQLXwNzF/CHpOZ5xsfzbp51AyngFXMiZYNmpAC+OQ7LIoukZLtyUMUyztxKUVMN/iko4OFWsxkiUldh/bWmgSJLMjSCRH1NeFm4pYUIRYHRDXFcI+O9mqYDsqWtjvS63B4AaEkiTV6Sm/Uxw9MMxRoWecw8tZoqypZkqKJZwzXQxAzHKsNkKWM6xdSYEZawhSfbz3NNQb93ubWkQ2uYxknVWlWYMmWosJu31n9kNMaGzKcxHIkwtsMWKEgAXZdFYmynnYnDybMSBYkkxjrndPb29lK9LkozupwsTTPOpdGmuJCEKUDYvlatW/7yrQeQsk1FgOZIJHM4o5tYVpqZagJQu4iQOxDw0J5h3UfDCo21/suQxdk5uh5yVR/A7ecFH0O7/QJgaa83F/URGk5Rp4UD8iBDpuxU1RUgQFIw8yydgoYsci6PxLr8EiRFb0Ai3QEzbXmP6CUZCUIaQp4XAv7/T5ncbqLHoWBAnKNMz1GWi0hJ4o6ONiGztC3dswCn1MZWBqKI7NNTrQoAydbo4gWLAovVQCR4u7VwQ/K1imwqkzDnz5UAVq3nRoRGDWEojoteXNB02KIpspCZWAvsMsQWYOBMHCqKaRgXybCuR3K1U+eI3h1tEnfHOpfn039yNt2QFbISdmKxssYjMwDQZco6fShBSzk1YF47nFatm+ZOG/W4eMvmnXVCsTZJDrGVxaHLJmVTplCPQBkQaWnDvGdTMev2ldC6oWqeZtjQFAUaHePLa6uaU6k0BrwCJ1pZ5yDBZTDBjlrGxwRoL+seJy/BPkg7J9qWlLuNuPCxO7xQO0d/3Ify7B8z6HiPHnQczXc46Gr6YaaSnjOpoL2wYHI5kjGkZCBRSO5SzW9WA1LdMKMD2ciflndpx8bG2vL62Cvz+xOZk5s/zJuc9baw7Ql65sjrQnEBU5L11EDPvt5TxEydMcVcaS/LMi5TR89yuvJ6byHzYOH0cPmP0/u7cOKAVPnFW1WPrDi+cOfL9JQRJ4ejKK9w0JFXsvusRJRMbavCX3aSnUgt69v/kK/s/otdu1rk5hU9bN+5/HdWbOGPNsHP2/t/2rHl5EuPbtr/SeX7Tza/qcyZuXuefnDTN3/0rS6d1bjuo33PfuyNvv0b9cHViw1/nz126NfVZ/dMu3BkxsFSMu/VnVUv7r/v53vXPHd1yaatxtL2xVtOvHteX//0DwvPvLHs8Lff/XkcriHx+dOB4s9WVTqLnunfXBQT13861zH7a8cLNXu/2ndl75mrezYmn0+d+mve7MfXlV0Jda1SBhbGDjdtPxFN1vceOFWz63L19z19i0Tf6a6jTq677cKS6LTL0/ecK31t+6zDVRsuHSvbWrPh0JF2+Rf+issbm/r6A8zQWv4Dp2Pu6PURAAA="


    scraped_df = pd.read_csv(PITSTOP_VEHICLE_TRIMS, dtype=str)
    scraped_df.drop_duplicates(subset=columns, inplace=True)
    
    
    vehicle_filter_df.merge(scraped_df[column], left_on=columns)

HEADERS = {
    "Authorization": f"Bearer {USER_TOKEN}",
    "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    "X-EBAY-C-ENDUSERCTX": "affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>",
}

DATA_FOLDER = "./data/eBay"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

os.makedirs(DATA_FOLDER, exist_ok=True)


class eBay:
    def __init__(
        self,
        query: str,
        postal_code: str,
        country: str,
        min_price: str = "",
        max_price: str = "",
        min_year: str = "2012",
        min_mileage: str = None,
        max_mileage: str = None,
        distance: str = "25",
        sort_by: str = "date",
        limit=40,
        vehicle_search=True,
    ):
        self.query = query
        self.postal_code = postal_code
        self.country = country
        self.distance = distance
        self.min_price = min_price
        self.max_price = max_price
        self.min_mileage = min_mileage
        self.max_mileage = max_mileage

        self.params = {
            "q": self.query,
            "limit": limit,
            "sort": "newlyListed",
            "filter": [
                f"pickupCountry:{self.country}",
                f"pickupPostalCode:{self.postal_code}",
                f"pickupRadius:{self.distance}",
                "pickupRadiusUnit:mi",
                "deliveryOptions:{SELLER_ARRANGED_LOCAL_PICKUP}",
            ],
        }

        # if vehicle_search:
        #     self.params["category_ids"] = 33559
        #     self.params["compatibility_filter"] = [f"Year:{min_year}"]

        if self.min_price or self.max_price:
            self.params.setdefault("filter", [])
            self.params["filter"].append(
                f"price:[{self.min_price}..{self.max_price}],priceCurrency:USD"
            )

    def new_listings_filename(self):
        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def get_listings(self):
        response = requests.get(
            API_URL,
            headers=HEADERS,
            params=self.params,
        )
        self.time_checked = datetime.now().timestamp()

        print(response.url)

        listings_df = self.parse_listing(response.json())
        return listings_df

    def check_new_listings(self):
        return
        latest_listings_df = self.get_listings()

        if os.path.exists(LISTINGS_DB):
            old_listings_df = pd.read_csv(LISTINGS_DB)

            latest_listings_df["country"] = latest_listings_df["country"].astype(float)
            latest_listings_df["poctal_code"] = latest_listings_df[
                "poctal_code"
            ].astype(float)
            latest_listings_df["listing_id"] = latest_listings_df["listing_id"].astype(
                int
            )

            old_listings_df["country"] = old_listings_df["country"].astype(float)
            old_listings_df["poctal_code"] = old_listings_df["poctal_code"].astype(
                float
            )
            old_listings_df["listing_id"] = old_listings_df["listing_id"].astype(int)

            merge_df = latest_listings_df.merge(
                old_listings_df[["listing_id", "query", "postal_code", "country"]],
                on=["listing_id", "query", "postal_code", "country"],
                how="left",
                indicator=True,
            )
            new_listings_df = merge_df[merge_df["_merge"] == "left_only"]

        else:
            new_listings_df = latest_listings_df

        ## Notify of new listings
        print(
            f"Found {len(new_listings_df)} new listings for {self.query} in lat {self.postal_code} and long {self.country}"
        )
        new_listings_df.to_csv(self.new_listings_filename(), index=None)

        ## Add new listing df to the listing db so notification doesnt come up again
        if "_merge" in new_listings_df.columns:
            new_listings_df = new_listings_df.drop(columns=["_merge"])

        new_listings_df.to_csv(
            LISTINGS_DB, mode="a", index=None, header=not os.path.exists(LISTINGS_DB)
        )

    def parse_listing(self, resp):
        with open("check_json.json", "w") as file:
            json.dump(resp, file, indent=4)

        listing_data = []
        items = resp.get("itemSummaries")
        for item in items:
            listing_id = item.get("itemId")
            title = item.get("title")
            url = item.get("itemWebUrl")
            seller = item.get("seller").get("username")

            price = item.get("price")
            p_value = price.get("value")
            p_currency = price.get("currency")

            price = f"{p_value} {p_currency}"

            thumbnail_images = item.get("thumbnailImages")
            images = [image.get("imageUrl") for image in thumbnail_images]

            location = item.get("itemLocation")
            postal_code = location.get("postalCode")
            country = location.get("country")
            location = f"{postal_code}, {country}"

            time_posted = item.get("itemCreationDate")
            listing_data.append(
                {
                    "query": self.query,
                    "poctal_code": self.postal_code,
                    "country": self.country,
                    "listing_id": listing_id,
                    "title": title,
                    "location": location,
                    "image_url": images[0] if images else None,
                    "price": price,
                    "url": url,
                    "time_posted": time_posted,
                    "seller": seller,
                }
            )
        listing_df = pd.DataFrame(listing_data)
        listing_df.drop_duplicates(inplace=True)
        return listing_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", help="Search Query", type=str, required=True)
    parser.add_argument(
        "-po",
        "--postal_code",
        help="Postal code of search location",
        type=str,
        default="11212",
    )
    parser.add_argument(
        "-co",
        "--country",
        help="COuntry of search location",
        type=str,
        default="US",
    )
    parser.add_argument(
        "-d",
        "--dist",
        help="Distance or search radius",
        type=str,
        default=f"{150*0.621371}",
    )
    parser.add_argument(
        "-min", "--min_price", help="Minimum Price of result", type=str, default=1000
    )
    parser.add_argument(
        "-max", "--max_price", help="Maximum Price of result", type=str, default=10
    )
    parser.add_argument(
        "-min_mileage",
        "--min_mileage",
        help="Minimum Mileage of Vehicle",
        type=str,
        default=0,
    )
    parser.add_argument(
        "-max_mileage",
        "--max_mileage",
        help="Maximum Mileage of Vehicle",
        type=str,
        default=400000 * 0.621371,
    )
    args = parser.parse_args()

    user_search = eBay(
        args.query,
        args.postal_code,
        args.country,
        # min_price=args.min_price,
        # max_price=args.max_price,
        min_mileage=args.min_mileage,
        max_mileage=args.max_mileage,
        distance=args.dist,
        min_year=2012,
    )
    user_search.check_new_listings()
