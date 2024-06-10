from geopy.geocoders import Nominatim

import requests
import json


API_URL = "https://www.kijiji.ca/anvil2/api"

HEADERS = {
    "Modane": "NTc5YjIzNTQtMGRkZC00YmRhLTkyZDUtZmRkNzU0ZGM3ZGI5",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
}


def get_location_id(location_name):
    place_id = get_place_suggestion(location_name)

    location = location_from_place(place_id)

    print(location)
    location_id = location.get("id")

    return location_id


def location_from_place(place_id):
    payload = {
        "query": "query locationFromPlace($placeId: String!, $sessionToken: String, $hints: [Hints]) {  locationFromPlace(placeId: $placeId, sessionToken: $sessionToken, hints: $hints) {    location {      ...LocationWithName      __typename    }    place {      ...LocationWithPlaceDetails      __typename    }    __typename  }}fragment LocationWithName on Location {  id  localizedName  __typename}fragment LocationWithPlaceDetails on PlaceDetails {  isCountry  isProvince  location {    latitude    longitude    __typename  }  __typename}",
        "variables": {"placeId": place_id},
    }

    response = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))

    resp_json = response.json()

    data = resp_json.get("data")
    location_from_place = data.get("locationFromPlace")
    location = location_from_place.get("location")

    return location


def get_place_suggestion(location_name):
    payload = {
        "query": "query PlaceSuggestions($input: String!, $location: LocationQueryCoordsOptions) {  placeSuggestions(input: $input, location: $location) {    placeId    address    __typename  }}",
        "variables": {"input": location_name},
    }

    response = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))

    resp_json = response.json()

    data = resp_json.get("data")

    place_suggestion = data.get("placeSuggestions")[0]

    place_id = place_suggestion.get("placeId")

    return place_id


def get_seo_url(
    query,
    location_id,
    min_price=None,
    max_price=None,
    min_mileage=None,
    max_mileage=None,
    min_year=None,
    max_year=None,
    car_search=False,
    limit=40,
):
    min_price = min_price * 100 if min_price != None else min_price
    max_price = max_price * 100 if max_price != None else max_price

    min_max_filter = {
        "filterName": "price",
        "minValue": min_price,
        "maxValue": max_price,
    }
    car_year_filter = {
        "filterName": "caryear",
        "minValue": min_year,
        "maxValue": max_year,
    }
    car_mileage_filter = {
        "filterName": "carmileageinkms",
        "minValue": min_mileage,
        "maxValue": max_mileage,
    }
    car_category = 174
    payload = {
        "query": "query GetSeoUrl($input: SearchUrlInput!) {searchUrl(input: $input)}",
        "variables": {
            "input": {
                "searchQuery": {
                    "rangeFilters": [min_max_filter],
                    "keywords": query,
                    "location": {"id": location_id},
                },
                "sorting": {"by": "DATE", "direction": "DESC"},
                "pagination": {"offset": 0, "limit": limit},
            }
        },
    }

    if car_search:
        payload["variables"]["input"]["searchQuery"]["categoryId"] = car_category
        payload["variables"]["input"]["searchQuery"]["rangeFilters"].append(
            car_year_filter
        )
        payload["variables"]["input"]["searchQuery"]["rangeFilters"].append(
            car_mileage_filter
        )

    response = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))

    resp_json = response.json()

    search_url = resp_json.get("data").get("searchUrl")
    return search_url
