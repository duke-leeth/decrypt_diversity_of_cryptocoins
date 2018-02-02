# -*- coding: utf-8 -*-
#
# TIME format: rfc3339

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

@app.route('/email')
def email():
     return render_template("base.html")


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/api/')
def api_index():
    return render_template('api_index.html', title = 'API')


@app.route('/api/coinlist/')
def get_coinlist():
    query = 'SELECT name, symbol, id FROM cryptcoin.basicinfo;'
    response = session.execute(query)
    response_list = []
    for val in response:
        response_list.append(val)

    jsonresponse = [{"name": x.name, "symbol": x.symbol,"id": x.id} for x in response_list]
    return jsonify(jsonresponse)


@app.route('/api/priceinfo/<string:coinid>/')
def get_priceinfo(coinid):
    no_rows = 25*12
    query = ("""
        SELECT id, time, price_usd, volume_usd_24h
        FROM priceinfo
        WHERE id='{Coin_Id}'
        LIMIT {Limit}
    """).format(Coin_Id=coinid, Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{'id': x.id, 'time': x.time.isoformat(), 'price_usd': x.price_usd, \
                    'volume_usd_24h': x.volume_usd_24h} for x in response_list]
    return jsonify(jsonresponse)


@app.route('/api/priceinfo/<string:coinid>/<string:start_time>/<string:end_time>/')
def get_priceinfo_timeperiod(coinid, start_time, end_time):
    query = ("""
        SELECT id, time, price_usd, volume_usd_24h
        FROM priceinfo
        WHERE id='{Coin_Id}'
            AND time > {Start_Time}
            AND time < {End_Time}
        ALLOW FILTERING
    """).format(Coin_Id=coinid, Start_Time=start_time, End_Time=end_time)\
        .translate(None, '\n')
    print query
    response = session.execute(query)

    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{'id': x.id, 'time': x.time.isoformat(), 'price_usd': x.price_usd, \
                    'volume_usd_24h': x.volume_usd_24h} for x in response_list]
    return jsonify(jsonresponse)





@app.route('/api/correlation/<coinid_a>/<coinid_b>')
def get_corr_coins(coinid_a, coinid_b):
    no_rows = 5*4
    query = ("""
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE id_a='{Coin_Id_A}' AND id_b='{Coin_Id_B}'
        LIMIT {Limit}
    """).format(Coin_Id_A=coinid_a, Coin_Id_B=coinid_b, Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                    'corr': x.corr} for x in response_list]
    return jsonify(jsonresponse)


@app.route('/api/correlation/<coinid_a>/<coinid_b>/<string:start_time>/<string:end_time>/')
def get_corr_coins_timeperiod(coinid_a, coinid_b, start_time, end_time):
    query = ("""
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE id_a='{Coin_Id_A}'
            AND id_b='{Coin_Id_B}'
            AND time > {Start_Time}
            AND time < {End_Time}
        ALLOW FILTERING
    """).format(Coin_Id_A=coinid_a, Coin_Id_B=coinid_b, \
                Start_Time=start_time, End_Time=end_time) \
        .translate(None, '\n')
    response = session.execute(query)

    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                    'corr': x.corr} for x in response_list]
    return jsonify(jsonresponse)


@app.route('/api/correlation/<string:start_time>/<string:end_time>/')
def get_corr_timeperiod(start_time, end_time):
    no_rows = 500
    query = ("""
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE time > {Start_Time} AND time < {End_Time}
        LIMIT {Limit}
        ALLOW FILTERING
    """).format(Start_Time=start_time, End_Time=end_time, Limit=no_rows) \
        .translate(None, '\n')
    response = session.execute(query)

    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                    'corr': x.corr} for x in response_list]
    return jsonify(jsonresponse)
