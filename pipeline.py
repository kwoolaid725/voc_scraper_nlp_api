import pandas as pd
import numpy as np

from scraper import AmazonScraper, BestBuyScraper, HomeDepotScraper

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import filter, col, first, round, concat, lit, when, to_date
import ssl

import nltk
from nltk.util import ngrams
from nltk import word_tokenize
from nltk.corpus import stopwords
from rake_nltk import Metric, Rake
import re
import string

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from tqdm.notebook import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
from wordcloud import WordCloud

from rake_nltk import Metric, Rake




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


# a = GroupbyRetail('amazon', 'LG A939KBGS', 'https://www.amazon.com/LG-CordZero-Cordless-Lightweight-Warranty/product-reviews/B0BZQT1235/ref=cm_cr_getr_d_paging_btm_next_10?ie=UTF8&reviewerType=all_reviews')()
# b = GroupbyRetail('bestbuy', 'LG A939KBGS', 'https://www.bestbuy.com/site/reviews/lg-cordzero-cordless-stick-vacuum-with-auto-empty-and-kompressor-sand-beige/6483115?variant=A&page=1')()
# # d = GroupbyRetail('homedepot', 'Dyson V10', 'https://www.homedepot.com/p/reviews/Dyson-V10-Animal-Cordless-Stick-Vacuum-394429-01/313126189/')()
# c = GroupbyRetail('homedepot', 'LG A939KBGS', 'https://www.homedepot.com/p/reviews/LG-CordZero-All-in-One-Cordless-Stick-Vacuum-Cleaner-A939KBGS/319148737/')()
#

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
        df_reviews = pd.DataFrame(reviews)
        return df_reviews


class NlpPipeline:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, regexp_replace
    from pyspark.sql.functions import filter, col, first, round, concat, lit, when, to_date
    def __init__(self, df_reviews):
        # group by retail from list_of_inputs
        self.df_reviews = df_reviews
        self.df = pd.DataFrame()
        self.preprocessed = False
        self.tokenized = False
        self.sentiment_done = False
    def preprocess(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, regexp_replace
        from pyspark.sql.functions import filter, col, first, round, concat, lit, when, to_date

        spark = SparkSession.builder.appName('NlpPipeline_preprocess').getOrCreate()
        sdf_preprocessed = spark.createDataFrame(self.df_reviews)
        sdf_preprocessed = sdf_preprocessed.\
            withColumn('CONTENT', regexp_replace('CONTENT', r'\[This review was collected as part of a promotion\.\]',''))
        # Just to show the number of dropped rows later
        original_count = sdf_preprocessed.count()

        # 2 - Dropping duplicate rows with the same 'REVIEWER_NAME', 'TITLE', 'CONTENT' values
        sdf_preprocessed = sdf_preprocessed.dropDuplicates(subset=['REVIEWER_NAME', 'TITLE', 'CONTENT'])
        new_count = sdf_preprocessed.count()
        dropped_count = original_count - new_count

        # Show the counts
        print(f"Original count: {original_count}")
        print(f"New count after dropping duplicates: {new_count}")
        print(f"Number of dropped duplicates: {dropped_count}")

        # 3 - Combine title and content columns
        sdf_preprocessed = sdf_preprocessed.withColumn('REVIEW', concat(col('TITLE'), lit(' '), col('CONTENT')))

        # 4 - Drop rows with empty 'REVIEW' column
        sdf_preprocessed = sdf_preprocessed.filter(col('REVIEW') != 'NaN NaN')
        print(f"Previous count: {new_count}")
        new_count = sdf_preprocessed.count()
        print(f"New count after drooping empty reviews: {new_count}")

        """ 
                5 - 'POST_DATE' column contains different formats from each retailer
                Converting them to 'yyyy-MM-dd' format

                POST_DATE formats
                Amazon: MMM dd, yyyy
                Best Buy: MMM dd, yyyy hh:mm a
                The Home Depot: MM dd, yyyy
        """

        # Set the legacy time parser policy
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

        # Assuming 'POST_DATE' is a string column containing date information
        sdf_preprocessed = sdf_preprocessed.withColumn(
            "POST_DATE",
            when(to_date('POST_DATE', 'MMM dd, yyyy hh:mm a').isNotNull(),
                 to_date('POST_DATE', 'MMM dd, yyyy hh:mm a'))
            .when(to_date('POST_DATE', 'MMM dd, yyyy').isNotNull(),
                  to_date('POST_DATE', 'MMM dd, yyyy'))
            .otherwise(None)  # If no format matches, set the column to None
        )

        # show sdf_preprocessed
        print(sdf_preprocessed.show(5))

        # 6 - to Pandas and Apply NLP library
        self.df = sdf_preprocessed.toPandas()

        # Format the 'POST_DATE' column using pandas.to_datetime()
        self.df['POST_DATE'] = pd.to_datetime(self.df['POST_DATE']).dt.strftime('%Y-%m-%d')

        spark.stop()
        self.preprocessed = True
        return self.df


    def process_words(self):
        if not self.preprocessed:
            raise ValueError("Preprocessing has not been done yet.")

        df = self.df.drop(columns=['TITLE', 'CONTENT'])
        df.insert(0, 'ID', range(0, len(df)))

        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context

        nltk.download()
        wn = nltk.WordNetLemmatizer()
        stopwords = nltk.corpus.stopwords.words('english')

        def clean_text(text):
            text = ''.join([word for word in text if word not in string.punctuation])
            tokens = re.split('\W+', text)
            text = [word for word in tokens if word not in stopwords]
            return text

        df['REVIEW_CLEAN'] = df['REVIEW'].apply(lambda x: clean_text(x.lower()))

        # Lemmatize the text
        def lemmatizing(tokenized_text):
            text = [wn.lemmatize(word) for word in tokenized_text]
            return text

        df['lemmatized'] = df['review_clean'].apply(lambda x: lemmatizing(x))
        df['review_clean'] = df.review_clean.apply(' '.join)

        # N-grams

        def extract_ngrams(data, num):
            n_grams = ngrams(nltk.word_tokenize(data), num)
            return [' '.join(grams) for grams in n_grams]

        df['ngram2'] = df['review_clean'].apply(lambda x: extract_ngrams(x, 2))
        self.df = df
        self.tokenized = True
        return df

    def sentiment_analysis(self):
        if not self.preprocessed & self.tokenized:
            raise ValueError("Preprocessing and Tokenizing have not been done yet.")
        def polarity_scores_roberta(review):
            model_name = f'cardiffnlp/twitter-roberta-base-sentiment'

            model = AutoModelForSequenceClassification.from_pretrained(model_name)
            tokenizer = AutoTokenizer.from_pretrained(model_name)

            encoded_text = tokenizer(review, padding=True, truncation=True, max_length=512, return_tensors='pt')

            output = model(**encoded_text)

            scores = output[0][0].detach().numpy()
            scores = softmax(scores)

            scores_dict = {
                'negative': scores[0],
                'neutral': scores[1],
                'positive': scores[2]
            }
            return scores_dict

        res = {}
        for i, row in tqdm(self.df.iterrows(), total=len(self.df)):
            try:
                text = row['REVIEW']
                myid = row['ID']
                roberta_result = polarity_scores_roberta(text)
                res[myid] = {**roberta_result}
            except RuntimeError:
                print(f'Broke for id {myid}')

        df_scores = pd.DataFrame(res).T
        df_scores = df_scores.reset_index().rename(columns={'index': 'ID'})
        df_merged = self.df.merge(df_scores, how='left')

        sns.pairplot(data=df_merged,
                     vars=['negative', 'neutral', 'positive'],
                     hue='RATING',
                     palette='tab10'
                     )
        fig = plt.gcf()
        fig.savefig("scores_seaborn.png")
        self.df = df_merged
        self.sentiment_done = True
        return df_merged


    def word_count(self):
        if not self.sentiment_done:
            raise ValueError("Sentiment Analysis has not been done yet.")
        df = self.df
        df['positivity'] = np.where((df['RATING'] >= 4) & (df['positive'] > 0.5), 1, 0)
        df['positivity'] = np.where((df['RATING'] <= 2) & (df['negative'] > 0.5), -1,
                                              df['positivity'])
        df['lemmatized_s'] = [', '.join(map(str, l)) for l in df['lemmatized']]
        df['ngram2_s'] = [', '.join(map(str, l)) for l in df['ngram2']]

        d = df.groupby(df['positivity']).agg({'lemmatized_s': lambda x: ', '.join(x),
                                                                  'ngram2_s': lambda x: ', '.join(x)})

        lem_pos = d['lemmatized_s'][1]
        lem_neu = d['lemmatized_s'][0]
        lem_neg = d['lemmatized_s'][-1]

        tags_pos = lem_pos.split(', ')  # Positivity [1]
        tags_neu = lem_neu.split(', ')  # Positivity [0]
        tags_neg = lem_neg.split(', ')  # Positivity [-1]
        res_pos = {}
        res_neu = {}
        res_neg = {}

        def word_count(tags, res):
            for i in tags:
                res[i] = tags.count(i)
            return res


        res_pos = word_count(tags_pos, res_pos)
        res_neu = word_count(tags_neu, res_neu)
        res_neg = word_count(tags_neg, res_neg)

        lemmatized_count = pd.DataFrame([res_pos, res_neu, res_neg]).astype('Int64').T.fillna(0)
        lemmatized_count.columns = ['POS(1)', 'NEU(0)', 'NEG(-1)']
        lemmatized_count = lemmatized_count.sort_values(by='POS(1)', ascending=False)
        lemmatized_count.name = 'Word Count by Sentiment'

        ngram2_pos = d['ngram2_s'][1]
        ngram2_neu = d['ngram2_s'][0]
        ngram2_neg = d['ngram2_s'][-1]

        tags_bi_pos = ngram2_pos.split(', ')  # Positive Bi-gram
        tags_bi_neu = ngram2_neu.split(', ')  # Neutral Bi-gram
        tags_bi_neg = ngram2_neg.split(', ')  # Negative Bi-gram

        res_bi_pos = {}
        res_bi_neu = {}
        res_bi_neg = {}

        res_bi_pos = word_count(tags_bi_pos, res_bi_pos)
        res_bi_neu = word_count(tags_bi_neu, res_bi_neu)
        res_bi_neg = word_count(tags_bi_neg, res_bi_neg)

        bigram_count = pd.DataFrame([res_bi_pos, res_bi_neu, res_bi_neg]).astype('Int64').T.fillna(0)
        bigram_count.columns = ['POS(1)', 'NEU(0)', 'NEG(-1)']
        bigram_count = bigram_count.sort_values(by='POS(1)', ascending=False)
        bigram_count.name = 'Bigram (2 adjacent words) Count by Sentiment'


        # word = lem_pos
        stopwords_c = [ 'x000d', 'love', 'good', 'great', 'product', 'get']


        wordcloud_pos = WordCloud(stopwords=stopwords_c, width=1000, height=500).generate(lem_pos)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud_pos)
        plt.axis("off")
        plt.savefig("wordcloud_pos.png", bbox_inches='tight')  # Save the figure


        wordcloud_neg = WordCloud(stopwords=stopwords_c, width=1000, height=500, colormap='RdPu').generate(lem_neg)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud_neg)
        plt.axis("off")
        plt.savefig("wordcloud_neg.png", bbox_inches='tight')  # Save the figure


        wordcloud_bi_pos = WordCloud(stopwords=stopwords_c, width=1000, height=500).generate_from_frequencies(
            res_bi_pos)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud_bi_pos)
        plt.axis("off")
        plt.savefig("wordcloud_bi_pos.png", bbox_inches='tight')  # Save the figure


        wordcloud_bi_neg = WordCloud(stopwords=stopwords_c, width=1000, height=500).generate_from_frequencies(
            res_bi_neg)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud_bi_neg)
        plt.axis("off")
        plt.savefig("wordcloud_bi_neg.png", bbox_inches='tight')

        self.df = df
        return df, wordcloud_pos, wordcloud_neg, wordcloud_bi_pos, wordcloud_bi_neg


    def keyword_extraction(self):

        if not self.sentiment_done:
            raise ValueError("Sentiment Analysis has not been done yet.")
        df = self.df
        # Keywords Extraction

        df['REVIEW'] = df['REVIEW'].apply(
            lambda x: x.replace(":   ", ":").replace(":  ", ":").replace(": ", ":").replace(":\n\n", ": ").
            replace(":\n",": ").replace("\t", "").replace("\n-", "").replace("\n ", "\n").replace("NaN", ""))

        # Divide REVIEW into PARAGRAPHS
        def separate_paragraphs(review):
            para_list = []
            paragraphs = review.split('\n\n')
            para_list.extend(paragraphs)
            return para_list

        df['PARAGRAPHS'] = df['REVIEW'].apply(lambda x: separate_paragraphs(x))

        # Drop df columns: REVIEW
        df_ = df.drop(columns=['RETAILER', 'PRODUCT', 'POST_DATE', 'REVIEWER_NAME', 'TITLE', 'CONTENT', 'REVIEW'])
        # explode the PARAGRAPHS Column
        df = df.explode('PARAGRAPHS')
        df = df.reset_index(drop=True)
        # remove empty rows from PARAGRAPHS
        df_paragraphs = df[df['PARAGRAPHS'] != '']

        df_paragraphs.insert(1, 'P_ID', range(0 + len(df_paragraphs)))

        res = {}
        for i, row in tqdm(df_paragraphs.iterrows(), total=len(df_paragraphs)):
            try:
                text = row['PARAGRAPHS']
                myid = row['P_ID']
                # vader_results = sia.polarity_scores(text)
                roberta_result = NlpPipeline.sentiment_analysis.polarity_scores_roberta(text)
                res[myid] = {**roberta_result}
            except RuntimeError:
                print(f'Broke for id {myid}')

        df_para_sentiment = pd.DataFrame(res).T
        df_para_sentiment = df_para_sentiment.reset_index().rename(columns={'index': 'P_ID'})
        df_keywords = df_para_sentiment.merge(df_paragraphs, how='left')


        for i in range(0, len(df_keywords['PARAGRAPHS'])):
            df_keywords['PARAGRAPHS'][i] = df_keywords['PARAGRAPHS'][i].translate(
                str.maketrans('', '', string.punctuation))
            df_keywords['PARAGRAPHS'][i] = df_keywords['PARAGRAPHS'][i].replace('\n', '. ')
            df_keywords['PARAGRAPHS'][i] = df_keywords['PARAGRAPHS'][i].lower()
            # keywords_df['PARAGRAPHS'][i] = re.sub("['\"]","",keywords_df['PARAGRAPHS'][i])
            for j in re.findall('"([^"]*)"', df_keywords['PARAGRAPHS'][i]):
                df_keywords['PARAGRAPHS'][i] = df_keywords['PARAGRAPHS'][i].replace('"{}"'.format(j),
                                                                                    j.replace(' ', '_'))

        df_keywords['KEYWORD'] = df_keywords['PARAGRAPHS'].apply(lambda x: word_tokenize(x))
        english_stopwords = stopwords.words('english')
        for i in range(0, len(df_keywords['KEYWORD'])):
            df_keywords['KEYWORD'][i] = [w for w in df_keywords['KEYWORD'][i] if w.lower() not in english_stopwords]

        # remove duplicate df_keywords['KEYWORD']
        df_keywords['KEYWORD'] = df_keywords['KEYWORD'].apply(lambda x: list(dict.fromkeys(x)))


        r = Rake(include_repeated_phrases=False,
                 min_length=2,
                 ranking_metric=Metric.WORD_DEGREE)
        keywords = []
        for i in range(0, len(df_keywords['PARAGRAPHS'])):
            keyword = r.extract_keywords_from_text(df_keywords['PARAGRAPHS'][i])
            keyword = r.get_ranked_phrases()
            keywords.append(keyword)
        df_keywords['KEYWORDS'] = keywords

        return df_keywords

    # def yake_keywords(self):


class Export:
    def __init__(self, df_reviews):
        self.df_reviews = df_reviews

    def to_json(self):
        reviews_json = self.df_reviews.to_json(orient='records')
        with open(f'RetailsReviews_{today}.json', 'w') as f:
            f.write(reviews_json)
        return print('JSON file created')

    def to_excel(self):
        self.df_reviews.to_excel(f'RetailsReviews_{today}.xlsx', index=False)
        return print('Excel file created')

    def to_parquet(self):
        self.df_reviews.write.parquet(f'RetailsReviews_{today}.parquet')
        return print('Parquet file created')


if __name__ == "__main__":
   # df_reviews = RunScrapers(list_of_inputs)
   # export_instance = Export(df_reviews.run())
   # export_instance.to_json()
   # export_instance.to_excel()

   # Read the input Excel file
   df_reviews = pd.read_excel('./RetailsReviews.xlsx')

   # Create an instance of the NlpPipeline class and preprocess the data
   nlp_pipeline = NlpPipeline(df_reviews)
   a = nlp_pipeline.preprocess()

   # Save the preprocessed data to an Excel file
   a.to_excel('RetailsReviews_preprocessed.xlsx', )

   # Print the preprocessed data
   print(a)


