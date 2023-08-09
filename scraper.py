import re
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import json, os
import pandas as pd


from collections import defaultdict


class RetailerReviewScraper:
    def __init__(self, url, product):
        self.url = url
        self.product = product


class AmazonScraper(RetailerReviewScraper):
    def __init__(self, url, product):
        super().__init__(url, product)
        self.retailer = "AMAZON"
        self.soup = None

    def get_data(self):
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
            'Accept-Language': 'en-US, en;q=0.5'
        }
        payload = {'api_key': '0a53d12b168b28c41638451a545f7495', 'url': self.url, 'keep_headers': 'true'}

        for i in range(5):
            r = requests.get('http://api.scraperapi.com', params=payload, headers=HEADERS, timeout=60)
            print("status code received:", r.status_code)
            if r.status_code != 200:
                # saving response to file for debugging purpose.
                continue
            else:
                self.soup = BeautifulSoup(r.text, 'html.parser')
                break

    def get_reviews(self):
        regex = re.compile('.*customer_review-.*')
        results = []
        page_count = 1

        while True:
            last_page = self.soup.find('li', {'class': 'a-disabled a-last'})

            for review in self.soup.find_all('div', {'id': regex}):
                result = {
                    'RETAILER': self.retailer,
                    'PRODUCT': self.product,
                    'RATING': round(float(
                        review.find('i', {'data-hook': 'review-star-rating'}).text.replace('out of 5 stars',
                                                                                           '').strip())),
                    'POST_DATE': review.find('span', {'data-hook': 'review-date'}).text.replace(
                        'Reviewed in the United States on', '').strip(),
                    'REVIEWER_NAME': review.find('div', {'class': 'a-profile-content'}).text.strip(),
                    'TITLE': review.find('a', {'data-hook': 'review-title'}).text.strip(),
                    'CONTENT': review.find('span', {'data-hook': 'review-body'}).text.strip(),
                }
                results.append(result)
                print(result)

            if last_page:
                break
            else:
                page_count += 1
                print(f'Page: {page_count}')
                self.url = self.url.split('&pageNumber=')[0]
                self.url = self.url + f'&pageNumber={page_count}'
                self.get_data()

        return results


# url = "https://www.amazon.com/LG-CordZero-Cordless-Lightweight-Warranty/product-reviews/B0BZQT1235/ref=cm_cr_getr_d_paging_btm_next_10?ie=UTF8&reviewerType=all_reviews"
# product_name = "LG CordZero Cordless Lightweight Warranty"
# amazon_scraper = AmazonScraper(url, product_name)
# amazon_scraper.get_data()
# amazon_reviews = amazon_scraper.get_reviews()

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

# url = 'https://www.bestbuy.com/site/reviews/lg-cordzero-cordless-stick-vacuum-with-auto-empty-and-kompressor-sand-beige/6483115?variant=A&page=1'
#
# product_name = "A939KBGS"
# bestbuy_scraper = BestBuyScraper(url, product_name)
# bestbuy_scraper.get_data()
# bestbuy_reviews = bestbuy_scraper.get_reviews()
class HomeDepotScraper(RetailerReviewScraper):
    def __init__(self, url, product):
        super().__init__(url, product)
        self.retailer = "THE HOME DEPOT"
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
        post_dates = []
        page_count = 1

        while True:

            data = self.soup.find_all('div', {'class': 'review_item'})
            for da in data:
                author = da.select_one(
                    'div > div >  div:nth-child(2) > div > div > div.review-content__no-padding.col__12-12 > button').text
                title = da.select_one('div > div > div:nth-child(2) > div > div > span').text
                date = da.select_one('div > div > div > div > span').text

                date_result = {
                    'REVIEWER_NAME': author,
                    'TITLE': title,
                    'POST_DATE': date
                }
                post_dates.append(date_result)

            json_data = [
                json.loads(x.string) for x in self.soup.find_all('script', type='application/ld+json')
            ]
            reviews = json_data[0]['review']

            for review in reviews:
                rating = (review['reviewRating']['ratingValue'])
                author = (review['author']['name'])
                title = (review['headline'])
                body = (review['reviewBody'])

                result = {
                    'RETAILER': self.retailer,
                    'PRODUCT': self.product,
                    'RATING': rating,
                    'REVIEWER_NAME': author,
                    'TITLE': title,
                    'CONTENT': body
                }

                results.append(result)
                print(result)

            if data:
                page_count += 1
                print(f'Page: {page_count}')
                # split last occurrence of '/' and add page count
                self.url = self.url.rsplit('/', 1)[0]
                self.url = self.url + f'/{page_count}'
                self.get_data()
            else:
                break
        df1 = pd.DataFrame(post_dates)
        df2 = pd.DataFrame(results)  # Use for Star Rating Counts

        # Dropping Unnecessary Rows
        df_with_reviews = df2[~df2.CONTENT.str.contains('Rating provided by a verified purchaser', na=False)]
        df_with_reviews.dropna(subset=['CONTENT'], inplace=True)

        df = df_with_reviews.merge(df1, how='inner', on=['REVIEWER_NAME', 'TITLE'])
        column_move = df.pop('POST_DATE')
        df.insert(3, 'POST_DATE', column_move)

        df_rating = df2['RATING']

        return df, df_rating

url = 'https://www.homedepot.com/p/reviews/LG-CordZero-All-in-One-Cordless-Stick-Vacuum-Cleaner-A939KBGS/319148737/'
product_name='LG A939KBGS'

homedepot_scraper = HomeDepotScraper(url, product_name)
homedepot_scraper.get_data()
homedepot_df = homedepot_scraper.get_reviews()
print(homedepot_df)