import scrapy
from scrapy import Spider
from scrapy import signals
import pandas as pd
from scrapy.crawler import CrawlerProcess


def strip_string(s):
    s = s.replace('\n', '')\
        .replace(' ', '')\
        .replace(':', '')\
        .replace('.', '')\
        .replace(',', '')\
        .replace('\\S+', '')\
        .replace('\r', '')
    return s


class BaseCrawler(Spider):

    name = "BaseCrawler"

    def __init__(
            self,
            base_url,
            num_pages,
            links_selector,
            selectors,
            article_identifier_word,
            data_path,
            chunks=5000
    ):
        super(BaseCrawler, self).__init__()
        self.base_url = base_url
        self.num_pages = num_pages
        self.links_selector = links_selector
        self.selectors = selectors
        self.chunks = chunks
        self.chunk_counter = chunks
        self.article_identifier_word = article_identifier_word
        self.data_path = data_path.replace('.csv', '')
        self.data_version = 1
        self.extracted_data = []

    def start_requests(self):
        try:
            for i in range(1, self.num_pages):
                u = self.base_url + str(i)
                yield scrapy.Request(url=u, callback=self.parse)
        except Exception as e:
            print(f'An exception has occurred: {e}')

    def _extract_information_from_page(self, response):
        result = {}
        for i, selector in enumerate(self.selectors):
            if i == 0:
                feature_name = 'Цена'
                try:
                    feature_value = strip_string(response.css(selector)[0].get())
                except Exception as e:
                    feature_value = 'По договор'
                result[feature_name] = feature_value
            elif i == 1:
                feature_name1 = 'Латитуда'
                feature_name2 = 'Лонгитуда'
                fv = response.css(selector)[0].get()
                fv = fv.split('q=')[1]
                fv = fv.split(',')
                result[feature_name1] = fv[0]
                result[feature_name2] = fv[1]
            else:
                elements = response.css(selector).getall()
                for j in range(0, len(elements) - 1, 2):
                    feature_name = strip_string(elements[j])
                    feature_value = strip_string(elements[j + 1])
                    result[feature_name] = feature_value

        return result

    def parse(self, response, **kwargs):
        if self.article_identifier_word not in response.url:
            links = response.css(self.links_selector).getall()
            for link in links:
                next_page = response.urljoin(link)
                yield scrapy.Request(next_page, callback=self.parse)
        else:
            try:
                res = self._extract_information_from_page(response)
                self.extracted_data.append(res)

                self.chunk_counter -= 1

                if self.chunk_counter <= 0:
                    pd.DataFrame(self.extracted_data).to_csv(f'{self.data_path}_{self.data_version}.csv')
                    self.data_version += 1
                    self.chunk_counter = self.chunks
                    self.extracted_data = []

            except Exception as e:
                print(f'An exception has occurred: {e} for url: {response.url}')
                pd.DataFrame(self.extracted_data).to_csv(f'{self.data_path}_{self.data_version}.csv')
                self.extracted_data = []
                self.data_version += 1

    def flush_spider(self):
        pd.DataFrame(self.extracted_data).to_csv(f'{self.data_path}_{self.data_version}.csv')
        self.extracted_data = []
        self.data_version = 1
        self.chunk_counter = self.chunks


def spider_ended(spider, reason):
    spider.flush_spider()


if __name__ == '__main__':
    # houses
    # base_url = 'https://reklama5.mk/Search?city=&cat=158&q=&f41_from=' \
    #            '&f41_to=&f42_from=&f42_to=&priceFrom=&priceTo=&f43=' \
    #            '&f44=&f10017=&f10018=&f10028=&sell=0&sell=1&buy=0&rent=0' \
    #            '&includeforrent=0&trade=0&includeOld=0&includeOld=1&includeNew=0' \
    #            '&includeNew=1&private=0&company=0&&SortByPrice=0&zz=1&pageView=&page='

    # flats
    # base_url = 'https://reklama5.mk/Search?city=&cat=159&q=&sell=0' \
    #            '&sell=1&buy=0&rent=0&includeforrent=0&trade=0&includeOld=0' \
    #            '&includeOld=1&includeNew=0&includeNew=1&private=0&company=0' \
    #            '&SortByPrice=0&zz=1&pageView=&page='
    # num_pages = 280  # 280 for houses

    # cars
    base_url = 'http://reklama5.mk/Search?city=&cat=24&q=&f31=&priceFrom=&' \
               'priceTo=&f33_from=&f33_to=&f36_from=&f36_to=&f35=&f37=&f138=' \
               '&f10016_from=&f10016_to=&f10042=&sell=0&sell=1&buy=0&trade=0' \
               '&includeOld=0&includeNew=0&private=0&company=0&SortByPrice=0' \
               '&zz=1&pageView=&page='
    num_pages = 980 # 980 for cars
    
    # the first selector should be the price
    selectors = [
        'body > div.container.body-content > '
        'div:nth-child(7) > div.row.mt-2 > div >'
        ' div > div.card-body.px-0 > div.card.border-0.bg-light.pl-2 >'
        ' div > div:nth-child(1) > div > '
        'div.col-md-7.defaultBlue.ml-2 > h5::text',  # price
        '#mapDiv > a::attr(href)',  # map
        'body > div.container.body-content >'
        ' div:nth-child(7) > div.row.mt-2 > div > '
        'div > div.card-body.px-0 > div:nth-child(4) > '
        'div.col-8 > div.row.mt-3 > div > p.mb-1::text',
    ]

    process = CrawlerProcess()

    process.crawl(
        BaseCrawler,
        base_url=base_url,
        num_pages=num_pages,
        links_selector='.SearchAdTitle::attr(href)',
        selectors=selectors,
        article_identifier_word='AdDetails',
        data_path='./data/cars.csv',  # ../data/cars.csv
        chunks=15000
    )

    for _c in process.crawlers:
        _c.signals.connect(spider_ended, signal=scrapy.signals.spider_closed)

    process.start()
