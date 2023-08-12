import pandas as pd
import json
import openpyxl
from scraper import AmazonScraper, BestBuyScraper, HomeDepotScraper
# today's date
from datetime import date
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import filter, col, first, round, concat, lit
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF


import nltk
import re
import string



today = date.today()

class UserInput:
    def __init__(self, retail, product_name, url):
        self.retail = retail
        self.product_name = product_name
        self.url = url

    def __call__(self, *args, **kwargs):
        a = self.retail + '_url'
        b = {a: self.url, 'product_name': self.product_name}
        return b


# merge dictionaries
# append dictionaries to list
list_of_inputs = []
class GroupbyRetail(UserInput):

    def __init__(self, retail, product_name, url):
        super().__init__(retail, product_name, url)
        self.retail = retail
        self.product_name = product_name
        self.url = url
        self.passed_dict = UserInput(self.retail, self.product_name, self.url)()


        list_of_inputs.append(self.passed_dict)


a = GroupbyRetail('amazon', 'LG A939KBGS', 'https://www.amazon.com/LG-CordZero-Cordless-Lightweight-Warranty/product-reviews/B0BZQT1235/ref=cm_cr_getr_d_paging_btm_next_10?ie=UTF8&reviewerType=all_reviews')()
b = GroupbyRetail('bestbuy', 'LG A939KBGS', 'https://www.bestbuy.com/site/reviews/lg-cordzero-cordless-stick-vacuum-with-auto-empty-and-kompressor-sand-beige/6483115?variant=A&page=1')()
# d = GroupbyRetail('homedepot', 'Dyson V10', 'https://www.homedepot.com/p/reviews/Dyson-V10-Animal-Cordless-Stick-Vacuum-394429-01/313126189/')()
c = GroupbyRetail('homedepot', 'LG A939KBGS', 'https://www.homedepot.com/p/reviews/LG-CordZero-All-in-One-Cordless-Stick-Vacuum-Cleaner-A939KBGS/319148737/')()


class RunScrapers:

    def __init__(self, list_of_inputs):
        # group by retail from list_of_inputs
        self.grouped_inputs = {}

        for input_dict in list_of_inputs:
            self.retail_key = next(key for key in input_dict.keys() if key.endswith('_url'))
            self.retail_name = self.retail_key.split('_url')[0]

            if self.retail_name not in self.grouped_inputs:
                self.grouped_inputs[self.retail_name] = []

            self.grouped_inputs[self.retail_name].append(input_dict)
        print(self.grouped_inputs)

    def run(self):

        agg_reviews = []
        if self.grouped_inputs.get('amazon') != None:
            for url in self.grouped_inputs['amazon']:
                for key, value in url.items():
                    if key == 'amazon_url':
                        self.amazon_url = value
                    if key == 'product_name':
                        self.product_name = value
                scraper = AmazonScraper(self.amazon_url, self.product_name)
                scraper.get_data()
                reviews = scraper.get_reviews()
                agg_reviews.append(reviews)

        if self.grouped_inputs.get('bestbuy') != None:
            for url in self.grouped_inputs['bestbuy']:
                for key, value in url.items():
                    if key == 'bestbuy_url':
                        self.bestbuy_url = value
                    if key == 'product_name':
                        self.product_name = value
                scraper = BestBuyScraper(self.bestbuy_url, self.product_name)
                scraper.get_data()
                reviews = scraper.get_reviews()
                agg_reviews.append(reviews)

        if self.grouped_inputs.get('homedepot') != None:
            for url in self.grouped_inputs['homedepot']:
                for key, value in url.items():
                    if key == 'homedepot_url':
                        self.homedepot_url = value
                    if key == 'product_name':
                        self.product_name = value
                scraper = HomeDepotScraper(self.homedepot_url, self.product_name)
                scraper.get_data()
                reviews = scraper.get_reviews()
                agg_reviews.append(reviews)
        else:
            print("No URLs provided")

        reviews = [item for sublist in agg_reviews for item in sublist]
        reviews_df = pd.DataFrame(reviews)
        return reviews_df


# ps = nltk.PorterStemmer()
# stopwords = nltk.corpus.stopwords.words('english')
# spark = SparkSession.builder.appName('reviews').getOrCreate()

# class Preprocess:
#
#     def __init__(self, reviews_df):
#         # to PySpark DataFrame
#         self.prepocessed_df = spark.createDataFrame(reviews_df)
#
#     # 1 - Removing "[This review was collected as part of a promotion."] from Home Depot reviews
#         self.prepocessed_df = self.prepocessed_df.withColumn('CONTENT', regexp_replace('CONTENT', 'This review was collected as part of a promotion.', ''))
#     # 2 - Dropping empty reviews in content column
#         self.prepocessed_df = self.prepocessed_df.filter(self.prepocessed_df.CONTENT != '')
#     # 3 - Dropping duplicate reviews
#         self.prepocessed_df = self.prepocessed_df.dropDuplicates(subset=['REVIEWER_NAME', 'TITLE', 'CONTENT'])
#     # 4 - Combine title and content columns
#         self.prepocessed_df = self.prepocessed_df.withColumn('REVIEW', concat(col('TITLE'), lit(' '), col('CONTENT')))

    # def clean_text(self):
    #     # 5 - Tokenize text
    #     tokenizer = Tokenizer(inputCol='REVIEW', outputCol='tokens')
    #
    #     text = ''.join([word for word in text if word not in string.punctuation])
    #     tokens = re.split('\W+', text)
    #     text = [word for word in tokens if word not in stopwords]
    #
    #


class Export:
    def __init__(self, reviews_df):
        self.reviews_df = reviews_df

    def to_json(self):
        reviews_json = self.reviews_df.to_json(orient='records')
        with open(f'RetailsReviews_{today}.json', 'w') as f:
            f.write(reviews_json)
        return print('JSON file created')

    def to_excel(self):
        self.reviews_df.to_excel(f'RetailsReviews_{today}.xlsx', index=False)
        return print('Excel file created')

    def to_parquet(self):
        self.reviews_df.write.parquet(f'RetailsReviews_{today}.parquet')
        return print('Parquet file created')


if __name__ == "__main__":
   reviews_df = RunScrapers(list_of_inputs)
   export_instance = Export(reviews_df.run())
   export_instance.to_json()
   export_instance.to_excel()



