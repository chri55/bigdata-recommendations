from flask import Flask, render_template, url_for
import dash
import dash_html_components as html
from dashboard import draw_dashboard
server = Flask(__name__)

@server.route("/")
def home():
    return render_template("home.html")

@server.route("/about")
def about():
    return render_template("about.html")

@server.route("/recommend")
def recommend():
    return render_template("recommendations.html")

app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/dashboard/'
)

draw_dashboard(app)

if __name__=="__main__":
    app.run_server(debug=True)
