from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import col, rand
from flask import Flask, request, render_template, render_template_string


def getRecommendations(userReviews, ratingtable):
    """
    Returns books based on what the user likes and dislikes

    list ratings: [("asin", "overall")]
    """


    data_to_train = []
    book = 0
    for e in userReviews:
        data_to_train.append(("asin_{}".format(book), e[0], e[1], "test_user", 2000000001, "fuckall"))

    df = spark.createDataFrame(data_to_train, ["asin", "asin_index", "overall", "reviewerID", "reviewerID_index", "title"])


    '''
        ratingtable = ratingtable.union(df)

        print("instance load")
        als = ALS(maxIter=20, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop")
        print("fit model")
        als_model = als.fit(ratingtable)
        #print("show preds")
        #preds.show()
    '''
    print("recommend for user subset")
    recs = als_model.recommendForUserSubset(df, 5)

    asins = []

    #print(recs)
    print("collecting")
    reclist = recs.where(recs.reviewerID_index == 2000000001).select("recommendations").limit(1).collect()
    print(reclist)
    print("collected dataframe")

    for e in reclist[0].asDict()["recommendations"]:
        print("getting title")
        i = e.asDict(recursive=True)
        print("got title")
        asins.append(ratingtable.where(ratingtable.asin_index == int(i["asin_index"])).select("title").collect())

    print(asins)

    return asins


spark = (
            SparkSession.builder.master("yarn")
                .config("spark.executor.memory", "10g")
                .config("spark.driver.memory", "60g")
                .config("spark.memory.offHeap.enabled",True)
                .config("spark.memory.offHeap.size","16g")
                .getOrCreate()
        )

#print("CREATE SPARK SESSION")
#print("SET SPARK CONFIGS")

#meta = spark.read.json("gs://metabooks/*.json")
#print("LOADING ratingtable")
ratingtable = spark.read.json("gs://als-output/als-preprocessed-nonull/*.json")
#print("LOADED ratingtable")

#print("CACHING ratingtable")
ratingtable.cache()
#print("CACHED ratingtable")

#print("LOADING metabooks")
meta = spark.read.json("gs://metabooks/*.json")
#print("LOADED METABOOKS")

#print("CACHING  metabooks")
meta.cache()
#print("CACHED metabooks")

app = Flask(__name__)

@app.route('/recommend', methods=['GET', 'POST'])  #can set first param to '/'
def toyFunction():
    global ratingtable
    global meta

    recommendHTML = '''
        <!DOCTYPE html>
        <html lang="en" dir="ltr">
          <head>
            <meta charset="utf-8">
            <title>Book Recommendation Dashboard</title>
            <style>
                img {
                    height: 200px;
                    width: 100px;
                }
                p {
                    font-size: 1.5em;
                }
            </style>
          </head>
          <body>
            <h1>Recommended Book For You</h1>
            <p> You gave these reviews: <p>
            <ul>
                {% for i in len %}
                <li>{{ old_titles[i][0].title }} : <span style="color: red;">{{ old_titles[i][1].rating }}</span></li>
                {% endfor %}
            </ul>
            <p>Based on other users with similar reviews, you might like</p>
            <ul>
                {% for i in len %}
                <img src="{{ imgUrls[i][0].imUrl }}" alt="[no cover image found]" />
                <li>{{ new_titles[i] }}</li>
                {% endfor %}
            </ul>
            <br/>
            <br/>
            <a href="http://0.0.0.0:5001/">Back to Home Page</a>
            <a href="http://0.0.0.0:5001/tables">See Analysis of All of the Amazon Data</a>
          </body>
        </html>
    '''

    if request.method == "POST":
        result = request.form
        #print(type(result))
        #print(result.to_dict())
        ratings = []
        old_titles = []
        for i in range(5):
            ratings.append((float(result["book{}".format(i)]), int(result["rating{}".format(i)])))
            old_titles.append((ratingtable.where(result["book{}".format(i)] == ratingtable.asin_index).select("title").limit(1).collect(), result["rating{}".format(i)]))
        ret = []
        #processed_data = getRecommendations(ratings, ratingtable)

        ## MAIN PROCESS
        data_to_train = []
        old_indices = []
        book = 0
        for e in ratings:
            old_indices.append(e[0])
            data_to_train.append(("unknown_asin", e[0], e[1], "test_user", 2000000001, "random_title"))

        df = spark.createDataFrame(data_to_train, ["asin", "asin_index", "overall", "reviewerID", "reviewerID_index", "title"])
        #df.show()
        #als_model = ALSModel.load("gs://als-output/gg-no-re")
        #preds = als_model.transform(df)
        #preds.show()

        ratingtable = ratingtable.union(df)

        #print("instance init")
        als = ALS(maxIter=20, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop")
        #print("fit model")
        als_model = als.fit(ratingtable.select("reviewerID_index", "asin_index", "overall"))
        #print("show preds")
        #preds.show()

        #print("recommend for user subset")
        recs = als_model.recommendForUserSubset(df, 5)

        asins = []
        imgUrls = []

        #print(recs)
        #print("collecting")
        #recs.show()
        reclist = recs.collect()
        #print(reclist)
        #print("collected dataframe")

        for e in reclist[0].asDict()["recommendations"]:
            #print("getting title")
            i = e.asDict(recursive=True)
            #print("got title")
            asins.append(ratingtable.where(ratingtable.asin_index == int(i["asin_index"])).select("asin", "title").collect())


        #print(asins)

        for e in asins:
            if len(e[0].title) != 0:
                ret.append(e[0].title)
                imgUrls.append(meta.where(e[0].asin == meta.asin).select("imUrl").limit(1).collect())

        #print(old_titles, imgUrls)

        return render_template_string(recommendHTML, new_titles=ret, old_titles=old_titles, imgUrls=imgUrls, len=range(len(ret)))

@app.route('/', methods=['GET', 'POST'])
def home():
    books = ratingtable.select("asin", "asin_index", "title").orderBy(rand()).limit(5).collect()

    imgUrls = []
    for i in range(len(books)):
        imgUrls.append(meta.where(meta.asin == books[i].asin).select("imUrl").collect())
        #print(imgUrls[i][0].imUrl)

    homeHTML = '''
    <!DOCTYPE html>
    <html lang="en" dir="ltr">
      <head>
        <meta charset="utf-8">
        <title>Book Recommendation Dashboard</title>
        <style>
            img {
                height: 200px;
                width: 100px;
            }
            p {
                font-size: 1.5em;
            }
        </style>
      </head>
      <body>
        <h1>Type a book name or get random suggestions to rate.</h1>
        <p> The application will recommend books it thinks you will like based on other similar users' experiences.</p>
        <form action = "http://0.0.0.0:5001/recommend" method="POST">
            {% for i in len %}
                <p>
                    <img src="{{ imgs[i][0].imUrl }}" alt="[no cover image found]"/>
                    <br />
                    <br />
                    How would you rate: {{ books[i].title }} ?
                    <input type="text" name="book{{ i }}" value={{ books[i].asin_index }} style="display:none;" />
                    <select  name="rating{{ i }}">
                        <option value="5" name="5">5</option>
                        <option value="4" name="4">4</option>
                        <option value="3" name="3">3</option>
                        <option value="2" name="2">2</option>
                        <option value="1" name="1">1</option>
                    </select>

                </p>
            {% endfor %}
            <input type="submit" value="Submit"/>
        </form>
        <br/>
        <br/>
        <a href="http://0.0.0.0:5001/tables">See Analysis of All of the Amazon Data</a>
      </body>
    </html>
    '''
    return render_template_string(homeHTML, books=books, len=range(len(books)), imgs=imgUrls)

@app.route("/tables", methods=['GET'])
def showTables():
        tableHTML = '''
        <!DOCTYPE html>
        <html lang="en" dir="ltr">
          <head>
            <meta charset="utf-8">
            <title>Book Recommendation Dashboard</title>
            <style>
                img {
                    height: 200px;
                    width: 100px;
                }
                p {
                    font-size: 1.5em;
                }
            </style>
          </head>
          <body>
            <iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/1uOxvMAwkfyJTOPE_shhDNGjFW_zbM0Bs/page/PaO8" frameborder="0" style="border:0" allowfullscreen></iframe>
            <iframe width="700" height="525" src="https://datastudio.google.com/embed/reporting/12NAPAT-M0QaDGDKCgoXbtOO06Lb6N6ic/page/rXO8" frameborder="0" style="border:0" allowfullscreen></iframe>
            <iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/13P6whnnaji7jAN-oLkwV1pxAMvI--BG8/page/MUO8" frameborder="0" style="border:0" allowfullscreen></iframe>
            <iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/1cfRRAXzoc0shAJTkCDIm19aRHbC1_NRj/page/56U8" frameborder="0" style="border:0" allowfullscreen></iframe>
            <br/>
            <br/>
            <a href="http://0.0.0.0:5001/">Back to Home Page</a>
          </body>
        </html>
        '''
        return render_template_string(tableHTML)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
