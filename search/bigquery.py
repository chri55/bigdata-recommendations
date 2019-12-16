from google.cloud import bigquery
from google.oauth2 import service_account
import six
import pandas as pd
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

def send_query(query_str):
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json')
    project_id = 'big-data-project-259918'
    client = bigquery.Client(credentials= credentials,project=project_id)

    query_job = client.query(query_str)

    results = query_job.result()  # Waits for job to complete.
    return results

def search_query(string):
    query_str = f"""
      SELECT imUrl, title, asin
      FROM `big-data-project-259918.bookreviews_AMZ.BookMetadata` WHERE title like '{ string }%'
      LIMIT 50
    """
    return send_query(query_str)

def book_query(string):
    query_str = f"""
      SELECT title, imUrl, description 
      FROM `big-data-project-259918.bookreviews_AMZ.BookMetadata` WHERE asin='{ string }'
    """
    return send_query(query_str)

def average_query(string):
    query_str = f"""
        SELECT count(*), avg(overall) FROM `big-data-project-259918.bookreviews_AMZ.BookReviewsFull` where asin='{ string }'
    """
    return send_query(query_str)

def review_query(string, n):
    query_str = f"""
        SELECT summary, reviewerName, reviewText, overall FROM `big-data-project-259918.bookreviews_AMZ.BookReviewsFull` where asin='{ string }' LIMIT { n }
    """
    return send_query(query_str)
