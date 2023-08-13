https://coderslegacy.com/python/scrapy-item-pipelines-and-processing/

scrapy startproject voc_scrapers
cd voc_scrapers
scrapy genspider voc_scraper


pip freeze > requirements.txt
deactivate
mv env env_old

pke:
pip install git+https://github.com/boudinfl/pke.git



Exception: No downloaded spacy model for 'en' language.
A list of downloadable spacy models is available at https://spacy.io/models.
Alternatively, preprocess your document as a list of sentence tuple (word, pos), such as:
	[[('The', 'DET'), ('brown', 'ADJ'), ('fox', 'NOUN'), ('.', 'PUNCT')]]python3 -m spacy download en_core_web_sm