from geopy.geocoders import Nominatim

import requests
import json


API_URL = "https://www.kijiji.ca/anvil2/api"

HEADERS = {
    "Modane": "NTc5YjIzNTQtMGRkZC00YmRhLTkyZDUtZmRkNzU0ZGM3ZGI5",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
}


def get_location_from_coordinates(latitude, longitude):
    return "Alb"


def get_location_id(latitude, longitude):
    location_name = get_location_from_coordinates(latitude, longitude)
    place_id = get_place_suggestion(location_name)

    location = location_from_place(place_id)
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

    pass
