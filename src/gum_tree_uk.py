import pandas as pd
from datetime import datetime, date, timedelta
import os
import requests
import argparse
import json
import math
import xml.etree.ElementTree as ET
import xml.dom.minidom


DATA_FOLDER = "./data/GumTreeUK"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

API_URL = "https://iphone-api.gumtree.com/capi/api/ads"

os.makedirs(DATA_FOLDER, exist_ok=True)


HEADERS = {
    "Accept": "*/*",
    "X-ECG-VER": "1.34",
    "X-ECG-AB-TEST-GROUP": "",
    "Accept-Language": "en-UK",
    "Accept-Encoding": "gzip",
    "User-Agent": "GumtreeUK 17.8.2 (iPhone; iOS 17.5.1; en_AU)",
    "Connection": "keep-alive",
    "X-ECG-USER-AGENT": "ios",
    "X-ECG-Experiments": "",
}


MILEAGE_DICT = {
    15000: "up_to_15000",
    30000: "up_to_30000",
    60000: "up_to_60000",
    80000: "up_to_80000",
    80001: "over_80000",
}


YEAR_DICT = {
    1: "up_to_1",
    2: "up_to_2",
    3: "up_t0_3",
    4: "up_to_4",
    5: "up_to_5",
    6: "up_to_6",
    7: "up_to_7",
    8: "up_to_8",
    9: "up_to_9",
    10: "up_to_10",
    11: "over_10",
}


RADIUS_DICT = {
    0: "zero",
    0.25: "quarter",
    0.5: "half",
    1: "one",
    3: "three",
    5: "five",
    10: "ten",
    15: "fifteen",
    30: "thirty",
    50: "fifty",
    75: "seventy_five",
    100: "hundred",
    150: "hundred_fifty",
    500: "five_hundred",
}


def get_year_difference_from_now(past_year):
    today = date.today()
    current_year = today.year
    return current_year - past_year


def get_closest_integer(integer_list, value, larger=True):
    """Gets the closest integer in the list to the given value.

    Parameters:
    - integer_list (list): List of integers to search through.
    - value (int): The reference value to find the closest integer to.
    - larger (bool): If True, find the closest larger integer; if False, find the closest smaller integer.

    Returns:
    - int: The closest integer to the value based on the specified direction.
    """
    if not integer_list:
        return None

    # Sort the list
    integer_list = sorted(integer_list)

    closest = None

    if larger:
        # Find the closest larger integer
        for num in integer_list:
            if num >= value:
                closest = num
                break
        else:
            return max(integer_list)
    else:
        # Find the closest smaller integer
        for num in reversed(integer_list):
            if num <= value:
                closest = num
                break
        else:
            return min(integer_list)

    return closest


def pretty_print_and_save_xml(element, file_name):
    # Convert the ElementTree object to a string
    rough_string = ET.tostring(element)

    # Use minidom to format the string
    reparsed = xml.dom.minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ")

    # Save the pretty XML to a file
    with open(file_name, "w") as file:
        file.write(pretty_xml)


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float):
    """
    Calculate the distance between two points

    @param lat1: Latitude of point 1
    @param lon1: Longitude of point 1
    @param lat2: Latitude of point 2
    @param lon2: Longitude of point 2
    @return: Distance between the points
    """
    earth_radius = 6371.0  # Radius of the earth in km

    lat1_rad = math.radians(float(lat1))
    lon1_rad = math.radians(float(lon1))
    lat2_rad = math.radians(float(lat2))
    lon2_rad = math.radians(float(lon2))

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = earth_radius * c

    return distance


# write a function, that takes in a number and get the year than number ago


class GumTreeUK:
    def __init__(
        self,
        query: str,
        lat: float,
        long: float,
        min_price: str = None,
        max_price: str = None,
        min_mileage: str = None,
        max_mileage: str = None,
        min_year: str = None,
        max_year: str = None,
        radius: int = 500,
        car_search: bool = False,
    ):
        self.query = query
        self.lat = lat
        self.long = long
        self.min_price = min_price
        self.max_price = max_price
        self.min_mileage = min_mileage
        self.max_mileage = max_mileage
        self.min_year = min_year
        self.max_year = max_year

        closest_radius = get_closest_integer(RADIUS_DICT.keys(), radius, larger=True)
        print(RADIUS_DICT[closest_radius])

        self.params = {
            "includeTopAds": "true",
            "nearby": "true",
            "distance": RADIUS_DICT[closest_radius],
            "q": self.query,
            "size": "10",
            "ad-status": "ACTIVE",
            "sortType": "DATE_DESCENDING",
            "page": "0",
            "searchOptionsExactMatch": "true",
            "_in": "id,title,price,locations,pictures,start-date-time,user-id,link,account-id",
            "latitude": self.lat,
            "longitude": self.long,
            "categoryId": "2549",  # for sale category. Has to be there or filter do not worl
        }

        if self.min_price:
            self.params["minPrice"] = self.min_price

        if self.max_price:
            self.params["maxPrice"] = self.max_price

        if car_search:
            self.params["_in"] = (
                "id,title,price,ad-type,locations,pictures,start-date-time,user-id,link,attributes,account-id"
            )
            self.params["attr[seller_type]"] = "private"
            self.params["categoryId"] = "9311"

            if self.max_mileage:
                closest_mileage = get_closest_integer(
                    MILEAGE_DICT.keys(), self.max_mileage, larger=True
                )
                print(closest_mileage)
                self.params["attr[vehicle_mileage]"] = MILEAGE_DICT[closest_mileage]

            if self.max_year:
                year_diff = get_year_difference_from_now(self.max_mileage)
                closest_year_diff = get_closest_integer(
                    YEAR_DICT.keys(), year_diff, larger=False
                )  # verify which to use
                self.params["attr[vehicle_registration_year]"] = YEAR_DICT[
                    closest_year_diff
                ]
        print(self.params)

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

        # print(response.json())

        listings_df = self.parse_listing(response.content)
        return listings_df

    def check_new_listings(self):
        latest_listings_df = self.get_listings()
        # return

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

    def parse_listing(self, resp_text):
        root = ET.fromstring(resp_text)
        if car_search:
            ns = {
                "ns0": "http://www.ebayclassifiedsgroup.com/schema/ad/v1",
                "ns1": "http://www.ebayclassifiedsgroup.com/schema/types/v1",
                "ns2": "http://www.ebayclassifiedsgroup.com/schema/location/v1",
                "ns3": "http://www.ebayclassifiedsgroup.com/schema/attribute/v1",
                "ns4": "http://www.ebayclassifiedsgroup.com/schema/picture/v1",
            }
        else:
            ns = {
                "ns0": "http://www.ebayclassifiedsgroup.com/schema/ad/v1",
                "ns1": "http://www.ebayclassifiedsgroup.com/schema/types/v1",
                "ns2": "http://www.ebayclassifiedsgroup.com/schema/location/v1",
                "ns3": "http://www.ebayclassifiedsgroup.com/schema/picture/v1",
            }

        print(ns)
        pretty_print_and_save_xml(root, "check.xml")
        listing_data = []

        ads = root.findall("ns0:ad", ns)

        for ad in ads:
            listing_id = ad.get("id")
            title = ad.find("ns0:title", ns).text
            price = ad.find("ns0:price/ns1:amount", ns).text
            currency_code = ad.find(
                "ns0:price/ns1:currency-iso-code/ns1:value", ns
            ).text
            start_date_time = ad.find("ns0:start-date-time", ns).text
            user_id = (
                ad.find("ns0:user-id", ns).text if ad.find("ns0:user-id", ns) else None
            )
            account_id = ad.find("ns0:account-id", ns).text
            image_xpath = "ns3:pictures/ns3:picture/ns3:link[@rel='extrabig']"
            print(image_xpath)
            image_xpath = (
                image_xpath.replace("ns3", "ns4") if car_search else image_xpath
            )
            image = (
                ad.find(image_xpath, ns).get(
                    "href",
                )
                # if ad.find(image_xpath, ns) is not None
                # else None
            )
            print(image)

            url = (
                ad.find("ns0:link[@rel='self-public-website']", ns).get("href")
                # if ad.find("ns0:link[@rel='self-public-website']", ns) is not None
                # else None
            )
            print(url)

            locations = ad.find("ns2:locations", ns)
            localized_name = None
            lat = None
            long = None
            for location in locations.findall("ns2:location", ns):
                localized_name = location.find("ns2:localized-name", ns).text
                area = location.find("ns2:area", ns)
                if area is not None:
                    lat = area.find("ns2:lat", ns).text
                    long = area.find("ns2:lng", ns).text
                    break

            mileage = None
            year = None

            mileage = (
                ad.find(
                    "ns3:attributes//ns3:attribute[@name='vehicle_mileage']//ns3:value",
                    ns,
                ).text
                if ad.find(
                    "ns3:attributes//ns3:attribute[@name='vehicle_mileage']//ns3:value",
                    ns,
                )
                is not None
                else None
            )
            year = (
                ad.find(
                    'ns3:attributes//ns3:attribute[@name="vehicle_registration_year"]//ns3:value',
                    ns,
                ).text
                if ad.find(
                    'ns3:attributes//ns3:attribute[@name="vehicle_registration_year"]//ns3:value',
                    ns,
                )
                is not None
                else None
            )
            listing_data.append(
                {
                    "query": self.query,
                    "lat": self.lat,
                    "long": self.long,
                    "listing_id": listing_id,
                    "title": title,
                    "location": f"{lat}, {long}" if lat and long else None,
                    "location_name": localized_name,
                    "image_url": image,
                    "price": f"{price} {currency_code}",
                    "url": url,
                    "time_posted": start_date_time,
                    "time_found": self.time_checked,
                    "mileage": mileage,
                    "year": year,
                    "distance": calculate_distance(self.lat, self.long, lat, long)
                    if lat and long
                    else None,
                    "account_id": account_id,
                    "user": user_id,
                }
            )
        listing_df = pd.DataFrame(listing_data)
        listing_df.drop_duplicates(inplace=True)
        return listing_df


if __name__ == "__main__":
    query = "toyota"
    lat = "51.5074"
    long = "-0.1278"
    min_price = 1000
    max_price = 10000
    min_mileage = 10000
    max_mileage = 150000
    distance = 50
    car_search = True
    min_year = 2022
    max_year = 2015

    user_search = GumTreeUK(
        query=query,
        lat=lat,
        long=long,
        min_price=min_price,
        max_price=max_price,
        min_mileage=min_mileage,
        max_mileage=max_mileage,
        max_year=max_year,
        # min_year=min_year,
        radius=distance,
        car_search=car_search,
    )
    user_search.check_new_listings()
