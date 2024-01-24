import pandas as pd
import numpy as np

from scraper import AmazonScraper, BestBuyScraper, HomeDepotScraper
from datetime import date
import ssl

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import filter, col, first, round, concat, lit, when, to_date, trim

import nltk
from nltk.util import ngrams
from nltk import word_tokenize
from nltk.corpus import stopwords
from rake_nltk import Metric, Rake
import re
import string

# from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoModelForSequenceClassification, BitsAndBytesConfig, AutoModelForCausalLM
from scipy.special import softmax
from tqdm.notebook import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
from wordcloud import WordCloud

from rake_nltk import Metric, Rake
from pke.unsupervised import YAKE
import yake

from yake.highlight import TextHighlighter
from IPython.display import HTML

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

nltk.download('wordnet')


today = date.today()


# model_name = f'cardiffnlp/twitter-roberta-base-sentiment'
model_name = f'cardiffnlp/twitter-xlm-roberta-base-sentiment'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
def polarity_scores_roberta(review):


    encoded_text = tokenizer(review, padding=True, truncation=True, max_length=512, return_tensors='pt')

    output = model(**encoded_text)

    scores = output[0][0].detach().numpy()
    scores = softmax(scores)

    scores_dict = {
        'NEG': scores[0],
        'NEU': scores[1],
        'POS': scores[2]
    }
    return scores_dict



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


a = GroupbyRetail('bestbuy', 'LG A931KWM', 'https://www.bestbuy.com/site/reviews/lg-cordzero-all-in-one-cordless-stick-vacuum-with-dual-floor-max-nozzle-essence-white/6553205?variant=A&page=1')()
# b = GroupbyRetail('bestbuy', 'Panasonic NN-SN77HS', 'https://www.bestbuy.com/site/reviews/panasonic-1-6-cu-ft-1250-watt-sn77hs-microwave-with-cyclonic-inverter-stainless-steel-silver/5834501?variant=A&page=1')()
# # d = GroupbyRetail('homedepot', 'Dyson V10', 'https://www.homedepot.com/p/reviews/Dyson-V10-Animal-Cordless-Stick-Vacuum-394429-01/313126189/')()
# c = GroupbyRetail('bestbuy', 'Panasonic NN-SE785S', 'https://www.bestbuy.com/site/reviews/panasonic-1-6-cu-ft-built-in-countertop-cyclonic-wave-microwave-oven-with-inverter-technology-stainless-steel/6258025?variant=A&page=1')()
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

    def __init__(self, df_reviews):
        # group by retail from list_of_inputs
        self.df_reviews = df_reviews
        self.df = pd.DataFrame()
        self.df_html = pd.DataFrame()
        self.preprocessed = False
        self.tokenized = False
        self.sentiment_done = False

    def preprocess(self):


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
        print(f"Previous count: {new_count}")
        sdf_filtered = sdf_preprocessed.filter(trim(col('CONTENT')) != 'NaN')
        sdf_preprocessed = sdf_filtered.withColumn('REVIEW', concat(col('TITLE'), lit('. '), col('CONTENT')))

        new_count = sdf_preprocessed.count()
        print(f"New count after drooping empty reviews: {new_count}")
        sdf_preprocessed = sdf_preprocessed.withColumn('REVIEW', regexp_replace('REVIEW', 'NaN', ''))


        """ 
                4 - 'POST_DATE' column contains different formats from each retailer
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
        return self


    def process_words(self):
        if not self.preprocessed:
            raise ValueError("Preprocessing has not been done yet.")

        df = self.df
        df.insert(0, 'ID', range(0, len(df)))

        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context

        # nltk.download(quiet=True)
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

        df['LEMMATIZED'] = df['REVIEW_CLEAN'].apply(lambda x: lemmatizing(x))
        df['REVIEW_CLEAN'] = df.REVIEW_CLEAN.apply(' '.join)

        # N-grams

        def extract_ngrams(data, num):
            n_grams = ngrams(nltk.word_tokenize(data), num)
            return [' '.join(grams) for grams in n_grams]

        df['NGRAM2'] = df['REVIEW_CLEAN'].apply(lambda x: extract_ngrams(x, 2))

        df['LEMMATIZED'] = [', '.join(map(str, l)) for l in df['LEMMATIZED']]
        df['NGRAM2'] = [', '.join(map(str, l)) for l in df['NGRAM2']]
        # df.drop(['LEMMATIZED', 'NGRAM2'], axis=1, inplace=True)

        self.df = df
        self.tokenized = True
        return self

    def sentiment_analysis(self):
        if not (self.preprocessed & self.tokenized):
            raise ValueError("Preprocessing and Tokenizing have not been done yet.")

        res = {}
        for i, row in tqdm(self.df.iterrows(), total=len(self.df)):
            try:
                text = row['REVIEW']
                myid = row['ID']
                roberta_result = polarity_scores_roberta(text)
                res[myid] = {**roberta_result}

            except RuntimeError as e:
                print(f'Error for id {myid}: {e}')
                # You can choose to handle the error gracefully or raise it again if needed

        print(res)
        df_scores = pd.DataFrame(res).T
        df_scores = df_scores.reset_index().rename(columns={'index': 'ID'})
        df_merged = self.df.merge(df_scores, how='left')

        sns.pairplot(data=df_merged,
                     vars=['NEG', 'NEU', 'POS'],
                     hue='RATING',
                     palette='tab10'
                     )
        fig = plt.gcf()
        fig.savefig("scores_seaborn.png")
        self.df = df_merged
        self.sentiment_done = True
        return self


    def word_count(self):
        if not self.sentiment_done:
            raise ValueError("Sentiment Analysis has not been done yet.")
        df = self.df
        print(df.head())
        df['POSITIVITY'] = np.where((df['RATING'] >= 4) & (df['POS'] > 0.5), 1, 0)
        df['POSITIVITY'] = np.where((df['RATING'] <= 2) & (df['NEG'] > 0.5), -1, df['POSITIVITY'])


        # d = df.groupby(df['POSITIVITY']).agg({'LEMMATIZED_S': lambda x: ', '.join(x),
        #                                                           'NGRAM2_S': lambda x: ', '.join(x)})
        #
        # lem_pos = d['LEMMATIZED_S'][1]
        # lem_neu = d['LEMMATIZED_S'][0]
        # lem_neg = d['LEMMATIZED_S'][-1]

        d = df.groupby(df['POSITIVITY']).agg({'LEMMATIZED': lambda x: ', '.join(x),
                                              'NGRAM2': lambda x: ', '.join(x)})

        lem_pos = d['LEMMATIZED'][1]
        lem_neu = d['LEMMATIZED'][0]
        lem_neg = d['LEMMATIZED'][-1]

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
        #
        # ngram2_pos = d['NGRAM2_S'][1]
        # ngram2_neu = d['NGRAM2_S'][0]
        # ngram2_neg = d['NGRAM2_S'][-1]

        ngram2_pos = d['NGRAM2'][1]
        ngram2_neu = d['NGRAM2'][0]
        ngram2_neg = d['NGRAM2'][-1]

        print(df.head())

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


        wordcloud_bi_neg = WordCloud(stopwords=stopwords_c, width=1000, height=500, colormap='RdPu').generate_from_frequencies(
            res_bi_neg)
        plt.figure(figsize=(15, 8))
        plt.imshow(wordcloud_bi_neg)
        plt.axis("off")
        plt.savefig("wordcloud_bi_neg.png", bbox_inches='tight')

        self.df = df
        return self


    def keyword_extraction(self):

        if not self.sentiment_done:
            raise ValueError("Sentiment Analysis has not been done yet.")
        df = self.df
        # Keywords Extraction

        df['REVIEW_P'] = df['REVIEW'].apply(
            lambda x: x.replace(":   ", ":").replace(":  ", ":").replace(": ", ":").replace(":\n\n", ": ").
            replace(":\n",": ").replace("\t", "").replace("\n-", " ").replace("\n ", "\n").replace("NaN", ""))

        # Divide REVIEW into PARAGRAPHS
        def separate_paragraphs(review):
            para_list = []
            paragraphs = review.split('\n\n')
            para_list.extend(paragraphs)
            return para_list


        df['PARAGRAPHS'] = df['REVIEW_P'].apply(lambda x: separate_paragraphs(x))
        df = df.drop(columns=['REVIEW_P'])

        # Drop df columns: REVIEW
        # df = df.drop(columns=['RETAILER', 'PRODUCT', 'POST_DATE', 'REVIEWER_NAME', 'TITLE',  'REVIEW'])
        # explode the PARAGRAPHS Column
        df = df.explode('PARAGRAPHS')
        df = df.reset_index(drop=True)

        for i in range(0, len(df['PARAGRAPHS'])):
            df['PARAGRAPHS'][i] = df['PARAGRAPHS'][i].translate(
                str.maketrans('', '', string.punctuation))
            df['PARAGRAPHS'][i] = df['PARAGRAPHS'][i].replace('\n', ', ')
            df['PARAGRAPHS'][i] = df['PARAGRAPHS'][i].lower()
            for j in re.findall('"([^"]*)"', df['PARAGRAPHS'][i]):
                df['PARAGRAPHS'][i] = df['PARAGRAPHS'][i].replace('"{}"'.format(j), j.replace(' ', '_'))

        df['KEYWORD'] = df['PARAGRAPHS'].apply(lambda x: word_tokenize(x))
        english_stopwords = stopwords.words('english')
        for i in range(0, len(df['KEYWORD'])):
            df['KEYWORD'][i] = [w for w in df['KEYWORD'][i] if w.lower() not in english_stopwords]

        #
        def separate_sentences(review):
            sent_list = []
            review = review.replace("but", ". but")
            review = review.replace("however", ". however")
            review = review.replace("although", ". although")
            review = review.replace("yet", ". yet")
            sentences = review.split('.')

            sent_list.extend(sentences)
            return sent_list

        df['SENTENCES'] = df['PARAGRAPHS'].apply(lambda x: separate_sentences(x))
        df = df.explode('SENTENCES')
        # df = df.reset_index(drop=True)
        print(df.head())

        # Drop df columns: PARAGRAPHS
        df = df.drop(columns=['PARAGRAPHS'])
        df = df[df['SENTENCES'] != '']
        # give id for each sentence in front of the sentence
        df.insert(0, 'SENT_ID', range(0 + len(df)))
        self.df = df
        df_sentences = df[['SENT_ID', 'SENTENCES']]
        res = {}
        for i, row in tqdm(df_sentences.iterrows(), total=len(df_sentences)):
            try:
                text = row['SENTENCES']
                myid = row['SENT_ID']
                # vader_results = sia.polarity_scores(text)
                roberta_result = polarity_scores_roberta(text)
                print(roberta_result)
                roberta_result = {k + '_SENT': v for k, v in roberta_result.items()}
                print(roberta_result)
                res[myid] = {**roberta_result}

            except RuntimeError:
                print(f'Broke for id {myid}')
        print(res)
        setiment_scores = pd.DataFrame(res).T
        setiment_scores = setiment_scores.reset_index().rename(columns={'index': 'SENT_ID'})
        df_sentences = df_sentences.merge(setiment_scores, how='left', on='SENT_ID')
        # df = df.merge(df_para_sentiment, how='left', on='SENT_ID')



        # # remove duplicate df_keywords['KEYWORD']
        # df_keywords['KEYWORD'] = df_keywords['KEYWORD'].apply(lambda x: list(dict.fromkeys(x)))


        # r = Rake(include_repeated_phrases=False,
        #          min_length=2,
        #          ranking_metric=Metric.WORD_DEGREE)
        # keywords_rake_2 = []
        # for i in range(0, len(df_sentences['SENTENCES'])):
        #     keyword = r.extract_keywords_from_text(df_sentences['SENTENCES'][i])
        #     keyword = r.get_ranked_phrases()
        #     keywords_rake_2.append(keyword)
        # df_sentences['KEYWORDS_RAKE(2)'] = keywords_rake_2

        # extractor = YAKE()

        keywords_yake = []
        keywords_yake_all = []



        language = "en"
        max_ngram_size = 2
        deduplication_threshold = 0.9
        deduplication_algo = 'seqm'
        windowSize = 1
        numOfKeywords = 5

        # if length of df_sentences is greater than 10

        df_sentences['WORD_COUNT'] = df_sentences['SENTENCES'].apply(lambda x: len(x.split()))


        for i in range(0, len(df_sentences['SENTENCES'])):

            if df_sentences['WORD_COUNT'][i] < 7:
                max_ngram_size = 1
                deduplication_threshold = 0.9
                deduplication_algo = 'seqm'
                windowSize = 1
                numOfKeywords = 2

                keywords = yake.KeywordExtractor(lan=language,n=max_ngram_size,
                                                        dedupLim=deduplication_threshold,
                                                        dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                        top=numOfKeywords, features=None).extract_keywords(df_sentences['SENTENCES'][i])

                keywords = [i[0] for i in keywords]
                keywords_yake.append(keywords)
                keywords = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                 dedupLim=deduplication_threshold,
                                                 dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                 top=numOfKeywords, features=None).extract_keywords(df_sentences['SENTENCES'][i])

                keywords_yake_all.append(keywords)


            elif df_sentences['WORD_COUNT'][i] < 20:
                max_ngram_size = 2
                deduplication_threshold = 0.9
                deduplication_algo = 'seqm'
                windowSize = 1
                numOfKeywords = 2

                keywords = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                 dedupLim=deduplication_threshold,
                                                 dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                 top=numOfKeywords, features=None).extract_keywords(
                    df_sentences['SENTENCES'][i])

                keywords = [i[0] for i in keywords]
                keywords_yake.append(keywords)
                keywords = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                 dedupLim=deduplication_threshold,
                                                 dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                 top=numOfKeywords, features=None).extract_keywords(
                    df_sentences['SENTENCES'][i])

                keywords_yake_all.append(keywords)

            else:
                max_ngram_size = 2
                deduplication_threshold = 0.9
                deduplication_algo = 'seqm'
                windowSize = 1
                numOfKeywords = 3

                keywords = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                 dedupLim=deduplication_threshold,
                                                 dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                 top=numOfKeywords, features=None).extract_keywords(
                    df_sentences['SENTENCES'][i])

                keywords = [i[0] for i in keywords]
                keywords_yake.append(keywords)
                keywords = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                 dedupLim=deduplication_threshold,
                                                 dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                 top=numOfKeywords, features=None).extract_keywords(
                    df_sentences['SENTENCES'][i])

                keywords_yake_all.append(keywords)

        df_sentences['KEYWORDS_YAKE'] = keywords_yake
        df_sentences['KEYWORDS_YAKE_SCOR'] = keywords_yake_all



        self.df_html = df_sentences

        df = df.merge(df_sentences, on='SENT_ID', how='left', suffixes=('', '_y'))
        df.drop(df.filter(regex='_y$').columns, axis=1, inplace=True)

        df.to_excel('output.xlsx', index=False)
        self.df = df


        return self

    def highlight_keywords(self):

        df = self.df_html

        css_style = """<head>
    <style>
        .positive {
            background-color: #66FF99;
            font-weight: bold;
        }
         .negative {
            background-color: #fd5c63;
            font-weight: bold;
        }
         .neutral {
            background-color: yellow;
            font-weight: bold;
        }
    </style>
</head>"""


        df['REVIEW_HIGHLIGHTED'] = css_style + '<body>' + df['SENTENCES'] + ' </body>'
        # df['REVIEW_HIGHLIGHTED'] = ''

        # css_style = """<head>
        #     <style> .positive {background-color: #66FF99; font-weight: bold; }</style>
        #          .negative { background-color: #ff6347; font-weight: bold;
        #         }
        #          .neutral {
        #             background-color: yellow;
        #             font-weight: bold;
        #         }
        #     </style>
        # </head>"""


        th_pos = TextHighlighter(max_ngram_size=2, highlight_pre="<span class='positive' >", highlight_post="</span>")
        th_neg = TextHighlighter(max_ngram_size=2, highlight_pre="<span class='negative' >", highlight_post="</span>")
        th_neu = TextHighlighter(max_ngram_size=2, highlight_pre="<span class='neutral' >", highlight_post="</span>")

        for i in range(0, len(df['KEYWORDS_YAKE_SCOR'])):
            if df['POS_SENT'][i] > 0.7:
                keywords = df['KEYWORDS_YAKE_SCOR'][i]
                df['REVIEW_HIGHLIGHTED'][i] = th_pos.highlight(df['REVIEW_HIGHLIGHTED'][i], keywords)
            elif df['NEG_SENT'][i] > 0.3:
                keywords = df['KEYWORDS_YAKE_SCOR'][i]
                df['REVIEW_HIGHLIGHTED'][i] = th_neg.highlight(df['REVIEW_HIGHLIGHTED'][i], keywords)
            elif df['NEU_SENT'][i] > 0.3:
                keywords = df['KEYWORDS_YAKE_SCOR'][i]
                df['REVIEW_HIGHLIGHTED'][i] = th_neu.highlight(df['REVIEW_HIGHLIGHTED'][i], keywords)
            else:
                pass

        df = df.drop(columns=['KEYWORDS_YAKE_SCOR', 'WORD_COUNT'])


        df_all = self.df[['ID', 'RETAILER', 'PRODUCT', 'POST_DATE', 'REVIEWER_NAME', 'RATING', 'TITLE','CONTENT','LEMMATIZED', 'NEG','NEU','POS', 'SENT_ID']]

        df_merged = df_all.merge(df, on='SENT_ID', how='left', suffixes=('', '_y'))

        # group by ID and concatenate the highlighted sentences
        df_merged = df_merged.groupby(['ID', 'RETAILER', 'PRODUCT', 'POST_DATE', 'REVIEWER_NAME', 'TITLE', 'CONTENT', 'RATING', 'POS','NEG', 'LEMMATIZED'])['REVIEW_HIGHLIGHTED'].apply(' '.join).reset_index()


        html_content = df_merged.to_html(escape=False, index=False)

        # write html to file
        with open('df_highlighted_final.html', 'w', encoding="utf-8") as f:
            f.write(html_content)
        df_merged.to_excel('df_highlighted_final.xlsx', index=False)
        self.df = df


        return self


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

   ''' Read the input Excel file '''
   df_reviews = pd.read_excel('./RetailsReviews_2024-01-24.xlsx')

   '''Create an instance of the NlpPipeline class and preprocess the data'''
   nlp_pipeline = NlpPipeline(df_reviews)
   # a = nlp_pipeline.preprocess().process_words().sentiment_analysis().keyword_extraction().highlight_keywords()
   a = nlp_pipeline.preprocess().process_words().sentiment_analysis().word_count()
   # get self.df from the NlpPipeline class




