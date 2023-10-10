import re
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import json, os

from zenrows import ZenRowsClient

class RetailerReviewScraper:
    def __init__(self, url, product):
        self.url = url
        self.product = product

class BestBuyScraper(RetailerReviewScraper):
    def __init__(self, url, product):
        super().__init__(url, product)
        self.retailer = "BEST BUY"
        self.soup = None

    def get_data(self):
        s = HTMLSession()
        for i in range(5):
            r = s.get(self.url, timeout=60)
            print("status code received:", r.status_code)
            if r.status_code != 200:
                # saving response to file for debugging purpose.
                continue
            else:
                self.soup = BeautifulSoup(r.text, 'html.parser')
                print(self.url)
                break

    def get_reviews(self):
        results = []
        page_count = 1

        while True:
            last_page = self.soup.find('li', {'class': 'page next disabled'})
            reviews = self.soup.select('.review-item')

            for review in reviews:
                data = review.find('script', type='application/ld+json')
                x = json.loads(data.string)
                rating = (x['reviewRating']['ratingValue'])
                date = review.find('time')['title']
                author = (x['author']['name'])
                title = (x['name'])
                body = (x['reviewBody'])

                result = {
                    'RETAILER': self.retailer,
                    'PRODUCT': self.product,
                    'RATING': rating,
                    'POST_DATE': date,
                    'REVIEWER_NAME': author,
                    'TITLE': title,
                    'CONTENT': body
                }
                results.append(result)
                print(result)

            if last_page:
                break
            else:
                page_count += 1
                print(f'Page: {page_count}')
                self.url = self.url.split('&')[0]
                self.url = self.url + f'&page={page_count}'
                self.get_data()

        return results


