import json
import string
import flask
import bigquery

from flask import request, render_template, redirect

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def listPage():
    result = bigquery.search_query(request.form['book'])
    return render_template('list.html', result = result)

@app.route('/books', methods=['GET', 'POST'])
def book():
    return redirect(request.url_root)

@app.route('/books/', methods=['GET', 'POST'])
def book2():
    return redirect(request.url_root)

@app.route('/books/<asin>', methods=['GET', 'POST'])
def books(asin):
    result = bigquery.book_query(asin)
    result = list(result)
    if not result:
        return render_template('404.html')
    title, cover, description = result[0]
    if not title:
        title = ''
    if not cover:
        cover = request.url_root + 'static/image404.png'
    result = bigquery.average_query(asin)
    result = list(result)
    review_no, avg_rating = result[0]
    if not review_no:
        review_no = 0
    if not avg_rating:
        avg_rating = 0
    else:
        avg_rating = round(avg_rating, 2)

    result = bigquery.review_query(asin, 10)
    #result = list(result)

    return render_template('books.html', asin=asin, title = title, cover = cover, description = description, review_no = review_no, avg_rating = avg_rating, review = result)

app.run()
# app.run()
# https://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask
