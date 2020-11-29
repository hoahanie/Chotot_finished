from __future__ import absolute_import
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.item import Item,Field
import urllib
import json
import datetime

class dulichSpider(scrapy.Spider):
    name = 'dulich'
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "origin": "https://xe.chotot.com",
        "pragma": "no-cache",
        "referer": "https://xe.chotot.com/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.2 Mobile/15E148 Safari/604.1"
    }

    base_url = 'https://gateway.chotot.com/v1/public/ad-listing'
    def start_requests(self):
        with open('city_name.json','r') as f:
            data = json.loads(f.read())
            for city in data:
                city_id = list(city.keys())[0]
                temp = city[city_id]
                seo_url = temp.lower().replace(" ","-")
                filename = './output/dulich/' + seo_url+'_' + datetime.datetime.today().strftime('%Y-%m-%d-%H-%M') + '.jsonl'
                query_string_params ='?cg=6000&region_v2={}&st=s,k&w=1&limit=50&w=1'.format(city_id)
                yield scrapy.Request(
                    url=self.base_url + query_string_params,
                    method='GET',
                    headers=self.headers,
                    callback=self.parse_page,
                    meta= {
                        'city_id': city_id,
                        'filename': filename
                    }
                )
    def parse_page(self, response):
        data = json.loads(response.body)
        city_id = response.meta["city_id"]
        filename = response.meta["filename"]
        total_ads = 0
        if ('total' in data.keys()):
            if(int(data['total'])>0):
                total_ads = data['total']
            else:
                return
        number_page = round(total_ads/50)

        for page in range(1,number_page+1):
            query_string_params = '?o={}&page={}&cg=6000&region_v2={}&st=s,k&w=1&limit=50&w=1'.format((page-1)*50,page,city_id)

            yield response.follow(
                    url=self.base_url + query_string_params,
                    method='GET',
                    headers=self.headers,
                    callback=self.parse_ads,
                    meta = {
                        "filename": filename
                    }
                )

    def parse_ads(self, response):
        data = json.loads(response.body)
        filename = response.meta["filename"]
        for ad in data['ads']:
            yield response.follow(
                url=self.base_url+ "/" + str(ad['list_id']),
                method='GET',
                headers=self.headers,
                callback=self.parse_ad_detail,
                meta = {
                        "filename": filename
                    }
                )

    def parse_ad_detail(self, response):
        data = json.loads(response.body)
        filename = response.meta["filename"]
        dictionary ={
                'list_id': data['ad']['list_id'],
                'list_time': data['ad']['list_time'],
                'name': data['ad']["account_name"],
                "account_id": data['ad']["account_id"],
                'phone': data['ad']["phone"],
                'address': data['ad']["area_name"] + "," + data['ad']["region_name"],
                'region_id': data['ad']["region_v2"]}
        with open(filename,"a") as f:
            f.write(json.dumps(dictionary) + '\n')

if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(dulichSpider)
    process.start()

        
