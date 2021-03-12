from flask import Flask, redirect, url_for
import flask
import json
from indexer import Indexer
from parse_html import Parser
from utils import get_logger
import time
app = Flask(__name__, static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')


myIndexer = Indexer('test')
parser = Parser()
logger = get_logger('APP')

@app.route('/')
def index():
    return redirect(url_for('search'))

@app.route('/search')
def search():
    return flask.render_template("index.html")

# Iftekhar ahmed
# machine learning
# ACM
# master of software engineering
@app.route('/api/search', methods=['POST'])
def query_api():
    data = flask.request.get_json()
    query = data['query']
    query = parser.stem_string(query)
    words = set(query.split())
    logger.info(query)

    T1 = time.clock()
    res = myIndexer.get_result(words)
    logger.info(f'time cost : {(time.clock() - T1) * 1000} ms')
    return json.dumps(res)
    # return json.dumps([{"docid":0, "score":1, "url":"a"},
    # {"docid":1, "score":2, "url":"c"},
    # {"docid":3, "score":2, "url":"d"},
    # {"docid":4, "score":2, "url":"v"},
    # {"docid":5, "score":6, "url":"c"},
    # {"docid":6, "score":2, "url":"c"}])