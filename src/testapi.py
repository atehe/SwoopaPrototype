import requests
import json


class KijijiAPI:
    def __init__(self):
        self.graphql_url = "https://www.kijiji.ca/anvil/api"
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        }
        self.proxies = {
            "http": "70f29264f96b68900a9b__cr.ca:7663b805c2a0a74f@gw.dataimpulse.com:823",
            "https": "70f29264f96b68900a9b__cr.ca:7663b805c2a0a74f@gw.dataimpulse.com:823",
        }

    # "location": {
    #                             "area": {
    #                                 "latitude": 49.2177586,
    #                                 "longitude": -122.4933584,
    #                                 "radius": 166
    #                             }
    #                         },
    def make_request(self):
        payload = {
            "operationName": "GetOrganicListings",
            "variables": {
                "input": {
                    "pagination": {"limit": 10, "offset": 0},
                    "sorting": {"by": "DATE", "direction": "DESC"},
                    "searchQuery": {
                        "attributeFilters": [
                            {
                                "filterName": "condition",
                                "values": ["usedgood", "usedfair", "usedlikenew"],
                            },
                            {"filterName": "for-sale-by", "values": ["ownr"]},
                        ],
                        "dateRangeFilters": [],
                        "keywords": "iphone",
                        "location": {"id": 9003},
                        "rangeFilters": [],
                    },
                }
            },
            "query": """
            query GetOrganicListings($input: SearchResultsInput!) {
              searchResultsPageUpdate(input: $input) {
                results {
                  organic {
                    ...Listing
                  }
                }
              }
            }
            
            fragment Listing on ListingV2 {
              id
              title
              price {
                amount
              }
              location {
                address
                distance
                name
              }
              imageUrls
              activationDate
              sortingDate
              posterInfo {
                id
              }
            }
            """,
        }

        response = requests.post(
            self.graphql_url,
            headers=self.headers,
            json=payload,  # The payload variable should be defined with your GraphQL query and variables
            proxies=self.proxies,
        )

        if response.status_code == 200:
            response_size_kb = len(response.content) / 1024
            print(
                f"Response size: {response_size_kb:.2f} KB"
            )  # Print the size of the response in KB
            return response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None


# Create an instance of the class and call the make_request method
kijiji_api_instance = KijijiAPI()
response = kijiji_api_instance.make_request()
print(
    json.dumps(response, indent=2)
)  # Print the formatted response or None if the request failed
