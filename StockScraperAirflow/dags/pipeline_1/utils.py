import re
# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
# from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sqlite3
import pandas as pd
from pipeline_1.news_factory import NewsFactory
import random



# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('vader_lexicon')


def fetch_data(**kwargs):
    print("starting the fetch data function....")
    news_source_list = ["yourstory", "finshots"]
    master_blog_text_dict = {}
    for news_source in news_source_list:
        source = NewsFactory.get_news_source(news_source)
        blog_text_dict = source.fetch_data()
        master_blog_text_dict[news_source] = blog_text_dict

    print("finished fetching data for all news sources")
    
    kwargs['ti'].xcom_push(key='articles', value=master_blog_text_dict)
    # return master_blog_text_dict

def clean_text(text):
    text = re.sub(r'\W', ' ', text)  # Remove special characters and punctuation
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
    # stop_words = set(stopwords.words('english'))
    # words = word_tokenize(text)
    # text = ' '.join([word for word in words if word not in stop_words])  # Remove stopwords
    return text


def preprocess_date(**kwargs):
    print("preprocessing the article data")
    master_blog_text_dict = kwargs['ti'].xcom_pull(key='articles', task_ids='fetch_data')
    cleaned_data = []
    for news_source, blog_text_dict in master_blog_text_dict.items():
        for ticker, content_list_dict in blog_text_dict.items():
            for d in content_list_dict:
                for title, content in d.items():
                    cleaned_title = clean_text(title)
                    cleaned_content = clean_text(content)
                cleaned_data.append({'news_source':news_source, 'ticker': ticker, 'title': cleaned_title, 'content': cleaned_content})

    # Deduplication based on title and content
    df = pd.DataFrame(cleaned_data).drop_duplicates(subset=['title', 'content'])
    print("article data cleaned")
    print(df.head(5))
    kwargs['ti'].xcom_push(key='cleaned_articles', value=df.to_dict('records'))
    # return df.to_dict('records')

def generate_sentiment_scores(**kwargs):
    print("generating sentiment analysis scores...")
    data = kwargs['ti'].xcom_pull(key='cleaned_articles', task_ids='preprocess_date')
    sentimented_data = []
    # sia = SentimentIntensityAnalyzer()
    for item in data:
        sentiment = random.uniform(0, 1)
        item['sentiment_score'] = sentiment
        sentimented_data.append(item)

    print(sentimented_data[0:5])
    print("sentiment scores generated")
    kwargs['ti'].xcom_push(key='sentiment_scores', value=sentimented_data)
    # return data

def store_data(**kwargs):
    print("storing data in db")
    data = kwargs['ti'].xcom_pull(key='sentiment_scores', task_ids='generate_sentiment_score')
    conn = sqlite3.connect('sentiment_scores.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sentiment_scores
                 (id INTEGER PRIMARY KEY, ticker TEXT, title TEXT, content TEXT, sentiment_score REAL)''')
    for item in data:
        c.execute("INSERT INTO sentiment_scores (ticker, title, content, sentiment_score) VALUES (?, ?, ?, ?)",
                  (item['ticker'], item['title'], item['content'], item['sentiment_score']))
    conn.commit()
    conn.close()
    print("data stored")
