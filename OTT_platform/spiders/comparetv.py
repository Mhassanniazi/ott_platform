import scrapy
import json
from bs4 import BeautifulSoup

class Comparetv(scrapy.Spider):
    name = "comparetv"
    url = "https://www.comparetv.com.au/streaming-search-library/"

    # within-script-configuration
    custom_settings = {
        'DOWNLOAD_DELAY' : 0.25,
        "RETRY_TIMES": 3,   
    }

    # defined headers
    headers = {
        'authority': 'www.comparetv.com.au',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en,en-US;q=0.9',
        'user-agent' : 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    }


    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, headers=self.headers)

    
    def parse(self,response):
        """ function to parse main page to get providers ids """
        provider_ids = response.css("select#pcs-provider>option:not(:first-child)::attr(value)").extract()
        provider_names = response.css("select#pcs-provider>option:not(:first-child)::text").extract()
        for index,id in enumerate(provider_ids):
            payload = {
                "action": "provider_content",
                "context": "search",
                "genre": "0",
                "limit": "20000",
                "page": "1",
                "provider": str(id),
                "sort": "latest",
                "type": "0"
            }
            yield scrapy.FormRequest(url="https://www.comparetv.com.au/wp-admin/admin-ajax.php", callback=self.parse_products, formdata=payload, meta={"platform" : provider_names[index]})


    def parse_products(self,response):
        """ to parse products listing """
        parsed_data = json.loads(response.text)
        raw_data = parsed_data.get("data")

        response = response.replace(body=raw_data)
        urls = response.css("div.search-content-item>a::attr(href)").extract()

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_page, headers=self.headers, meta={"platform" : response.meta.get("platform")})
    
    def parse_page(self,response):
        """ to parse particular page """
        soup = BeautifulSoup(response.text,"html.parser")
        all_tags = soup.find_all("script")

        for tag in all_tags[::-1]:
            if "@graph" in tag.text:
                break
        
        # parsing data dict
        parsed_data = json.loads(tag.text)
        base_data = parsed_data.get("@graph")[0]
        yield {
            "Title" : base_data.get("name"),
            "site name" : "comparetv",
            "Description" : base_data.get("description"),
            "Release Year" : base_data.get("dateCreated"),
            "Genre" : base_data.get("genre") and ", ".join(base_data.get("genre")),
            "Cast and Crew" : "".join(response.css("p.cast>span ::text").extract()),
            "IMDB rating" : response.css("img[src*='imdb']+span::text").extract_first() and response.css("img[src*='imdb']+span::text").extract_first().split("/")[0].strip(),
            "Duration" : None,
            "Image URL" : base_data.get("image"),
            "Platform" : response.meta.get("platform")
        }


# process = CrawlerProcess()
# process.crawl(spiderName)
# process.start()