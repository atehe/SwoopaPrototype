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

    def make_request(self, make='bmw', model='bmw', year_min=2000, year_max=2015,
                     conditions=['used', 'damaged', 'salvage', 'lease'],
                     seller_type='ownr', km_min=0, km_max=300000, price_min=0, price_max=80000, latitude=49.2177586,
                     longitude=-122.4933584, radius=111.0):
        condition_str = '__'.join(conditions)
        url = (
            f"https://www.kijiji.ca/b-cars-trucks/british-columbia/{make}/{model}"
            f"-{year_min}__{year_max}-{condition_str}/k0c174l9007a54a68a49?"
            f"for-sale-by={seller_type}&kilometers={km_min}__{km_max}&price={price_min}__{price_max}"
            f"&address={latitude}%2C{longitude}&radius={radius}"
        )
        payload = {
                    "operationName": "GetSearchResultsPageByUrl",
                    "variables": {
                        "searchResultsByUrlInput": {
                            "url": url,
                            "pagination": {"limit": 10, "offset": 0}
                        }
                    },    
                
            "query": """
                query GetSearchResultsPageByUrl($searchResultsByUrlInput: SearchResultsByUrlInput!) {
                searchResultsPageByUrl(input: $searchResultsByUrlInput) {
                    ...CoreSRP
                    searchQuery {
                    ...CoreSearchQuery
                    }
                }
                }

                fragment CoreSRP on SearchResultsPage {
                results {
                    organic {
                    ...Listing
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

                fragment CoreSearchQuery on SearchQuery {
                keywords
                }
    
            """
        }

        response = requests.post(
            self.graphql_url,
            headers=self.headers,
            json=payload,
            proxies=self.proxies
        )

        if response.status_code == 200:
            response_size_kb = len(response.content) / 1024  # Calculate the response size in KB
            return response.json(), response_size_kb  # Return both the response data and the size
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None, 0  # Return None for the data and 0 for the size

# Create an instance of the class and call the make_request method
kijiji_api_instance = KijijiAPI()
response_data, response_size_kb = kijiji_api_instance.make_request()

# Print the formatted response or None if the request failed
if response_data is not None:
    print(json.dumps(response_data, indent=2))
    print(f"Response size: {response_size_kb:.2f} KB")  # Print the response size in KB
else:
    print("No response data to display.")
