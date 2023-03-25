import scrapy
from scrapy.crawler import CrawlerProcess
import json
import mysql.connector as mysql
from bs4 import BeautifulSoup

class Justwatch(scrapy.Spider):
    name = "justwatch"                               # on-project-level: scrapy crawl <name>
    url = "https://apis.justwatch.com/graphql"

    # within-script-configuration
    custom_settings = {
        'DOWNLOAD_DELAY' : 0.25,
        "RETRY_TIMES": 3,
        "ROBOTSTXT_OBEY" : False

        # configuring output format, export as CSV format
        # "FEED_FORMAT" : "csv",
        # "FEED_URI" : "file.csv",
        # "FEED_EXPORT_ENCODING" : "utf-8-sig",         
    }
    headers = {
        'authority': 'apis.justwatch.com',
        'accept': '*/*',
        'accept-language': 'en,en-US;q=0.9',
        'content-type': 'application/json',
    }


    def start_requests(self):
        yield scrapy.Request(url=self.url, method="POST", body=self.generate_payload(""), headers=self.headers, callback=self.parse)

    
    def parse(self,response):
        """ function to parse listing page"""
        parsed_data = json.loads(response.text)
        next_page = parsed_data.get("data").get("popularTitles").get("pageInfo").get("endCursor")
        all_data = parsed_data.get("data").get("popularTitles").get("edges")

        for info in all_data:
            get_headers = {"authority":"www.justwatch.com","accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7","user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
            relative_path = info.get("node").get("content").get("fullPath")
            yield scrapy.Request(url=f"https://www.justwatch.com{relative_path}",headers=get_headers, callback=self.parse_page, meta={"title": info.get("node").get("content").get("title"), "IMBD_rating":info.get("node").get("content").get("scoring").get("imdbScore"), "platform" : info.get("node").get("watchNowOffer").get("package").get("clearName")})
         
        if next_page:
            yield scrapy.Request(url=self.url, callback=self.parse, headers=self.headers,method="POST", body=self.generate_payload(next_page))


    def parse_page(self,response):
        """" function to parse particular page """
        soup = BeautifulSoup(response.text, "html.parser")
        data = soup.find_all("script")[0].text
        try:
            parsed = json.loads(data)
            base = parsed.get("@graph")[0]

            description = base.get("description")
            genre = ", ".join(base.get("genre"))
            release_year = base.get("dateCreated")
            cast_crew = [{"character_name":item.get("characterName"), "original_name": item.get("actor").get("name") if item.get("actor") else item.get("name")} for item in base.get("actor")]

            yield {
                "Title" : response.meta.get("title"),
                "site name" : "justwatch",
                "Description" : description,
                "Release Year" : release_year,
                "Genre" : genre,
                "Cast and Crew" : str(cast_crew),
                "IMDB rating" : response.meta.get("IMBD_rating"),
                "Duration" : response.xpath("//div[h3[text()='چلنے کا دورانیہ ']]/following-sibling::div/text()").extract_first(),  # selector to get duration time
                "Image URL" : base.get("image").replace("s166","s592"),
                "Platform" : response.meta.get("platform")
            }
        except:
            print("❌ Proxies require for bulk scraping")

    def generate_payload(self,offset):
        """ function to manipulate payload for requests """
        payload = json.dumps({
            "operationName": "GetPopularTitles",
            "variables": {
                "popularTitlesSortBy": "POPULAR",
                "first": 230,     # limit
                "platform": "WEB", 
                "sortRandomSeed": 0,
                "popularAfterCursor": offset if offset else "",  # offset
                "popularTitlesFilter": {
                "ageCertifications": [],
                "excludeGenres": [],
                "excludeProductionCountries": [],
                "genres": [],
                "objectTypes": [],
                "productionCountries": [],
                "packages": [
                    "nfx",
                    "prv",
                    "zee"
                ],
                "excludeIrrelevantTitles": False,
                "presentationTypes": [],
                "monetizationTypes": []
                },
                "watchNowFilter": {
                "packages": [      # for all streamings
                    "nfx",
                    "prv",
                    "zee"
                ],
                "monetizationTypes": []
                },
                "language": "ur",
                "country": "PK"    # query results based on pak
            },
            "query": "query GetPopularTitles($country: Country!, $popularTitlesFilter: TitleFilter, $watchNowFilter: WatchNowOfferFilter!, $popularAfterCursor: String, $popularTitlesSortBy: PopularTitlesSorting! = POPULAR, $first: Int! = 40, $language: Language!, $platform: Platform! = WEB, $sortRandomSeed: Int! = 0, $profile: PosterProfile, $backdropProfile: BackdropProfile, $format: ImageFormat) {\n  popularTitles(\n    country: $country\n    filter: $popularTitlesFilter\n    after: $popularAfterCursor\n    sortBy: $popularTitlesSortBy\n    first: $first\n    sortRandomSeed: $sortRandomSeed\n  ) {\n    totalCount\n    pageInfo {\n      startCursor\n      endCursor\n      hasPreviousPage\n      hasNextPage\n      __typename\n    }\n    edges {\n      ...PopularTitleGraphql\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment PopularTitleGraphql on PopularTitlesEdge {\n  cursor\n  node {\n    id\n    objectId\n    objectType\n    content(country: $country, language: $language) {\n      title\n      fullPath\n      scoring {\n        imdbScore\n        __typename\n      }\n      posterUrl(profile: $profile, format: $format)\n      ... on ShowContent {\n        backdrops(profile: $backdropProfile, format: $format) {\n          backdropUrl\n          __typename\n        }\n        __typename\n      }\n      isReleased\n      __typename\n    }\n    likelistEntry {\n      createdAt\n      __typename\n    }\n    dislikelistEntry {\n      createdAt\n      __typename\n    }\n    watchlistEntry {\n      createdAt\n      __typename\n    }\n    watchNowOffer(country: $country, platform: $platform, filter: $watchNowFilter) {\n      id\n      standardWebURL\n      package {\n        packageId\n        clearName\n        __typename\n      }\n      retailPrice(language: $language)\n      retailPriceValue\n      lastChangeRetailPriceValue\n      currency\n      presentationType\n      monetizationType\n      availableTo\n      __typename\n    }\n    ... on Movie {\n      seenlistEntry {\n        createdAt\n        __typename\n      }\n      __typename\n    }\n    ... on Show {\n      seenState(country: $country) {\n        seenEpisodeCount\n        progress\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"
        })

        return payload


# process = CrawlerProcess()
# process.crawl(Justwatch)
# process.start()
