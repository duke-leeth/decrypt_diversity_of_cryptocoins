# jsonify creates a json representation of the response
from flask import jsonify

from flask import render_template
from app import app

from cassandra.cluster import Cluster

import config


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptcoin'
TABLE_NAME = 'priceinfo'

cluster = Cluster([CASSANDRA_DNS])
session = cluster.connect()
session.set_keyspace(KEYSPACE)


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html", title = 'Home')


@app.route('/email')
def email():
     return render_template("base.html")


@app.route('/api')
def api():
    query = "SELECT name, symbol, id FROM cryptcoin.basicinfo WHERE rank <= 10 ALLOW FILTERING;"
    response = session.execute(query)
    response_list = []

    print response

    for val in response:
        response_list.append(val)

    jsonresponse = [{"name": x.name, "symbol": x.symbol,"id": x.id} for x in response_list]
    return jsonify(jsonresponse)



@app.route('/api/id=<coinid>/')
def get_price(coinid):
    query = "SELECT id, time, price_usd FROM priceinfo WHERE id=%s LIMIT 30"
    response = session.execute(query, parameters=[coinid])
    response_list = []

    print response

    for val in response:
        response_list.append(val)
    jsonresponse = [{"id": x.id, "time": x.time,"price_usd": x.price_usd, "percent_change_1h": x.percent_change_1h} for x in response_list]
    return jsonify(jsonresponse)
