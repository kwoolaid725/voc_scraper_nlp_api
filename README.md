# voc_scraper_nlp_api

## <--- Update in Progress ---> 

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/d317d006-7014-4330-b416-0ead6b00c8dc)








## Preprocessing for NLP 

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/816869ae-330f-4932-8939-7341abd698f1)
![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/f716d857-792a-4ced-9e71-85744d604517)

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/c3f7d342-4cac-4cc7-9f46-05eef28bce79)

- Dropping NaN Reviews ( for both 'Title' and 'Content' empty)
![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/479611df-559d-4315-a27a-84b0ea2921b5)

- Post_date

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/e1b1265b-7188-4c17-b9c1-d79e3476ebc5)

It appears that the date format 'Apr 1, 2023' is causing an issue when trying to parse it using the new date parser introduced in Spark 3.0 due to inconsistencies in how certain date formats are handled by the new parser.
To resolve this, you can set the Spark configuration to use the legacy time parser policy by adding the following line: spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/adb31a53-2135-4f01-a262-c6ac29395335)

![image](https://github.com/kwoolaid725/voc_scraper_nlp_api/assets/107806433/846f3631-3165-4371-83d3-10a2b2be1f36)


a

