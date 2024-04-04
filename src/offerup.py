import requests
import json
import pandas as pd
from datetime import datetime
import os
import argparse

API_URL = "https://offerup.com/api/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}
DATA_FOLDER = "./data/offerup"
LISTINGS_DB = f"{DATA_FOLDER}/LISING_DB.csv"

os.makedirs(DATA_FOLDER, exist_ok=True)


class OfferUp:

    def __init__(
        self,
        query: str,
        lat: str,
        long: str,
        distance: str = "50",
        limit: str = "50",
        sort_by: str = "-posted",
    ):
        self.query = query
        self.lat = lat
        self.long = long
        self.distance = distance
        self.payload = {
            "query": "query GetModularFeed($searchParams: [SearchParam], $debug: Boolean = false) {  modularFeed(params: $searchParams, debug: $debug) {    analyticsData {      requestId      searchPerformedEventUniqueId      searchSessionId      __typename    }    categoryInfo {      categoryId      isForcedCategory      __typename    }    feedAdditions    filters {      ...modularFilterNumericRange      ...modularFilterSelectionList      __typename    }    legacyFeedOptions {      ...legacyFeedOptionListSelection      ...legacyFeedOptionNumericRange      __typename    }    looseTiles {      ...modularTileBanner      ...modularTileBingAd      ...modularTileGoogleDisplayAd      ...modularTileJob      ...modularTileEmptyState      ...modularTileListing      ...modularTileLocalDisplayAd      ...modularTileSearchAlert      ...modularTileSellerAd      ...modularModuleTileAdsPostXAd      __typename    }    modules {      ...modularGridModule      __typename    }    pageCursor    query {      ...modularQueryInfo      __typename    }    requestTimeMetadata {      resolverComputationTimeSeconds      serviceRequestTimeSeconds      totalResolverTimeSeconds      __typename    }    searchAlert {      alertId      alertStatus      __typename    }    debugInformation @include(if: $debug) {      rankedListings {        listingId        attributes {          key          value          __typename        }        __typename      }      lastViewedItems {        listingId        attributes {          key          value          __typename        }        __typename      }      categoryAffinities {        affinity        count        decay        affinityOwner        __typename      }      rankingStats {        key        value        __typename      }      __typename    }    __typename  }}fragment modularFilterNumericRange on ModularFeedNumericRangeFilter {  isExpandedHighlight  lowerBound {    ...modularFilterNumericRangeBound    __typename  }  shortcutLabel  shortcutRank  subTitle  targetName  title  type  upperBound {    ...modularFilterNumericRangeBound    __typename  }  __typename}fragment modularFilterNumericRangeBound on ModularFeedNumericRangeFilterNumericRangeBound {  label  limit  placeholderText  targetName  value  __typename}fragment modularFilterSelectionList on ModularFeedSelectionListFilter {  targetName  title  subTitle  shortcutLabel  shortcutRank  type  isExpandedHighlight  options {    ...modularFilterSelectionListOption    __typename  }  __typename}fragment modularFilterSelectionListOption on ModularFeedSelectionListFilterOption {  isDefault  isSelected  label  subLabel  value  __typename}fragment legacyFeedOptionListSelection on FeedOptionListSelection {  label  labelShort  name  options {    default    label    labelShort    selected    subLabel    value    __typename  }  position  queryParam  type  __typename}fragment legacyFeedOptionNumericRange on FeedOptionNumericRange {  label  labelShort  leftQueryParam  lowerBound  name  options {    currentValue    label    textHint    __typename  }  position  rightQueryParam  type  units  upperBound  __typename}fragment modularTileBanner on ModularFeedTileBanner {  tileId  tileType  title  __typename}fragment modularTileBingAd on ModularFeedTileBingAd {  tileId  bingAd {    ouAdId    adExperimentId    adNetwork    adRequestId    adTileType    adSettings {      repeatClickRefractoryPeriodMillis      __typename    }    bingClientId    clickFeedbackUrl    clickReturnUrl    contentUrl    deepLinkEnabled    experimentDataHash    image {      height      url      width      __typename    }    impressionFeedbackUrl    impressionUrls    viewableImpressionUrls    installmentInfo {      amount      description      downPayment      __typename    }    itemName    lowPrice    price    searchId    sellerName    templateFields {      key      value      __typename    }    __typename  }  tileType  __typename}fragment modularTileGoogleDisplayAd on ModularFeedTileGoogleDisplayAd {  tileId  googleDisplayAd {    ouAdId    additionalSizes    adExperimentId    adHeight    adNetwork    adPage    adRequestId    adTileType    adWidth    adaptive    channel    clickFeedbackUrl    clientId    contentUrl    customTargeting {      key      values      __typename    }    displayAdType    errorDrawable {      actionPath      listImage {        height        url        width        __typename      }      __typename    }    experimentDataHash    formatIds    impressionFeedbackUrl    personalizationProperties {      key      values      __typename    }    prebidConfigs {      key      values {        timeout        tamSlotUUID        liftoffPlacementIDs        __typename      }      __typename    }    renderLocation    searchId    searchQuery    templateId    __typename  }  tileType  __typename}fragment modularTileJob on ModularFeedTileJob {  tileId  tileType  job {    address {      city      state      zipcode      __typename    }    companyName    datePosted    image {      height      url      width      __typename    }    industry    jobId    jobListingUrl    jobOwnerId    pills {      text      type      __typename    }    title    apply {      method      value      __typename    }    wageDisplayValue    provider    __typename  }  __typename}fragment modularTileEmptyState on ModularFeedTileEmptyState {  tileId  tileType  title  description  iconType  __typename}fragment modularTileListing on ModularFeedTileListing {  tileId  listing {    ...modularListing    __typename  }  tileType  __typename}fragment modularListing on ModularFeedListing {  listingId  conditionText  flags  image {    height    url    width    __typename  }  isFirmPrice  locationName  price  title  vehicleMiles  __typename}fragment modularTileLocalDisplayAd on ModularFeedTileLocalDisplayAd {  tileId  localDisplayAd {    ouAdId    adExperimentId    adNetwork    adRequestId    adTileType    advertiserId    businessName    callToAction    callToActionType    clickFeedbackUrl    contentUrl    experimentDataHash    headline    image {      height      url      width      __typename    }    impressionFeedbackUrl    searchId    __typename  }  tileType  __typename}fragment modularTileSearchAlert on ModularFeedTileSearchAlert {  tileId  tileType  title  __typename}fragment modularTileSellerAd on ModularFeedTileSellerAd {  tileId  listing {    ...modularListing    __typename  }  sellerAd {    ouAdId    adId    adExperimentId    adNetwork    adRequestId    adTileType    clickFeedbackUrl    experimentDataHash    impressionFeedbackUrl    searchId    __typename  }  tileType  __typename}fragment modularModuleTileAdsPostXAd on ModularFeedTileAdsPostXAd {  ...modularTileAdsPostXAd  moduleId  moduleRank  moduleType  __typename}fragment modularTileAdsPostXAd on ModularFeedTileAdsPostXAd {  tileId  adsPostXAd {    ouAdId    adExperimentId    adNetwork    adRequestId    adTileType    clickFeedbackUrl    experimentDataHash    impressionFeedbackUrl    searchId    offer {      beacons {        noThanksClick        close        __typename      }      title      description      clickUrl      image      pixel      ctaYes      ctaNo      __typename    }    __typename  }  tileType  __typename}fragment modularGridModule on ModularFeedModuleGrid {  moduleId  collection  formFactor  grid {    actionPath    tiles {      ...modularModuleTileBingAd      ...modularModuleTileGoogleDisplayAd      ...modularModuleTileListing      ...modularModuleTileLocalDisplayAd      ...modularModuleTileSellerAd      __typename    }    __typename  }  moduleType  rank  rowIndex  searchId  subTitle  title  infoActionPath  __typename}fragment modularModuleTileBingAd on ModularFeedTileBingAd {  ...modularTileBingAd  moduleId  moduleRank  moduleType  __typename}fragment modularModuleTileGoogleDisplayAd on ModularFeedTileGoogleDisplayAd {  ...modularTileGoogleDisplayAd  moduleId  moduleRank  moduleType  __typename}fragment modularModuleTileListing on ModularFeedTileListing {  ...modularTileListing  moduleId  moduleRank  moduleType  __typename}fragment modularModuleTileLocalDisplayAd on ModularFeedTileLocalDisplayAd {  ...modularTileLocalDisplayAd  moduleId  moduleRank  moduleType  __typename}fragment modularModuleTileSellerAd on ModularFeedTileSellerAd {  ...modularTileSellerAd  moduleId  moduleRank  moduleType  __typename}fragment modularQueryInfo on ModularFeedQueryInfo {  appliedQuery  decisionType  originalQuery  suggestedQuery  __typename}",
            "variables": {
                "debug": False,
                "searchParams": [
                    {"key": "q", "value": self.query},
                    {"key": "platform", "value": "web"},
                    {"key": "lon", "value": self.long},
                    {"key": "lat", "value": self.lat},
                    {"key": "limit", "value": limit},
                    {"key": "SORT", "value": sort_by},
                    {"key": "DISTANCE", "value": self.distance},
                ],
            },
        }

    def new_listings_filename(self):

        return f"{DATA_FOLDER}/NEW_LISTINGS_{self.time_checked}.csv"

    def get_listings(self):
        response = requests.post(
            API_URL, headers=HEADERS, data=json.dumps(self.payload)
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

    def parse_listing(self, listing_json: dict):

        records = listing_json.get("data").get("modularFeed").get("looseTiles")
        listing_data = []

        for record in records:
            if record.get("tileType").lower() == "listing":
                listing = record.get("listing")
                listing_id = listing.get("listingId")
                condition_text = listing.get("conditionText")
                flags = ", ".join(listing.get("flags"))
                image_url = listing.get("image").get("url")
                is_firm_price = listing.get("isFirmPrice")
                location_name = listing.get("locationName")
                price = listing.get("price")
                title = listing.get("title")
                vehicle_miles = listing.get("vehicleMiles")
                url = f"https://offerup.com/item/detail/{listing_id}"

                listing_data.append(
                    {
                        "query": self.query,
                        "lat": self.lat,
                        "long": self.long,
                        "listing_id": listing_id,
                        "condition_text": condition_text,
                        "flags": flags,
                        "image_url": image_url,
                        "is_firm_price": is_firm_price,
                        "location_name": location_name,
                        "price": price,
                        "title": title,
                        "vehicle_miles": vehicle_miles,
                        "url": url,
                        "time_found": self.time_checked,
                    }
                )
        listing_df = pd.DataFrame(listing_data)
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

    user_search = OfferUp(args.query, str(args.lat), str(args.long))
    user_search.check_new_listings()
