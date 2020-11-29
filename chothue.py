from __future__ import absolute_import
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import urllib
import json
import datetime

import psycopg2
from sshtunnel import SSHTunnelForwarder
import sys


class chothuebds(scrapy.Spider):
    name = 'chothuebds'
    headers = {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "origin": "https://nha.chotot.com",
        "pragma": "no-cache",
        "referer": "https://nha.chotot.com/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36"
    }

    base_url = 'https://gateway.chotot.com/v1/public/ad-listing'
    
    def start_requests(self):
        with open('city_name.json','r') as f:
            data = json.loads(f.read())
            for city in data:
                city_id = list(city.keys())[0]
                temp = city[city_id]
                seo_url = temp.lower().replace(" ","-")
                filename = './output/chothue/' + seo_url+'_' + datetime.datetime.today().strftime('%Y-%m-%d-%H-%M') + '.jsonl'

                query_string_params = '?region_v2={}&cg=1000&limit=50&st=u,h'.format(city_id)
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
        city_id = response.meta['city_id']
        filename = response.meta['filename']
        total_ads = 0
        if ('total' in data.keys()):
            if(int(data['total'])>0):
                total_ads = data['total']
            else:
                return
        number_page = round(total_ads/50)
        for page in range(1,number_page+1):
            query_string_params ='?region_v2={}&cg=1000&limit=50&o={}&st=u,h&page={}'.format(city_id,(page-1)*50,page)
            yield response.follow(
                    url=self.base_url + query_string_params,
                    method='GET',
                    headers=self.headers,
                    callback=self.parse_ads,
                    meta={
                        'filename': filename
                    }
                )
    def parse_ads(self, response):
        data = json.loads(response.body)
        filename = response.meta['filename']
        for ad in data['ads']:
            yield response.follow(
                url=self.base_url+ "/" + str(ad['list_id']),
                method='GET',
                headers=self.headers,
                callback=self.parse_ad_detail,
                meta={
                        'filename': filename
                    }
                )

    def parse_ad_detail(self, response):
        data = json.loads(response.body)
        filename = response.meta['filename']
        with open(filename ,"a") as f:
            f.write(json.dumps(data) + '\n')

if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(chothuebds)
    process.start()



        
