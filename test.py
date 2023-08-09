from requests_html import HTMLSession
from bs4 import BeautifulSoup
# HTMLSession for status from 403 to 200
s = HTMLSession()
url = 'https://www.bestbuy.com/site/reviews/lg-cordzero-cordless-stick-vacuum-with-auto-empty-and-kompressor-sand-beige/6483115?variant=A&page=1'
r = s.get(url, timeout=60)
print(r)