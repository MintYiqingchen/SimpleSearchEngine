from flask import Flask, redirect, url_for
import flask
import json

app = Flask(__name__, static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')

@app.route('/')
def index():
    return redirect(url_for('search'))

@app.route('/search')
def search():
    return flask.render_template("index.html")

@app.route('/api/search', methods=['POST'])
def query_api():
    print(flask.request.get_json())
    return json.dumps([{"docid":0, "score":1, "url":"a"},
    {"docid":1, "score":2, "url":"c"},
    {"docid":3, "score":2, "url":"d"},
    {"docid":4, "score":2, "url":"v"},
    {"docid":5, "score":6, "url":"c"},
    {"docid":6, "score":2, "url":"c"}])