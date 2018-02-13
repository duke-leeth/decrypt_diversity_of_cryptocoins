# -*- coding: utf-8 -*-
#
# TIME format: rfc3339
import time
import ast

from flask import jsonify

from flask import render_template
from app import app

from cassandra.cluster import Cluster

import config
import id_info


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptocoins'

cluster = Cluster([CASSANDRA_DNS])
session = cluster.connect()
session.set_keyspace(KEYSPACE)

ID_DICT = id_info.ID_DICT
INV_ID_DICT = id_info.INV_ID_DICT
ID_LIST = id_info.ID_LIST



@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/api/')
def api_index():
    return render_template('api_index.html', title='API')


@app.route('/api/coinlist/')
def get_coinlist():
    query = 'SELECT name, symbol, id, rank FROM cryptocoins.basicinfo;'
    response = session.execute(query)
    response_list = []
    for val in response:
        response_list.append(val)

    BasicInfo_Dict = {}
    no_variables = 1000
    for x in response_list:
        BasicInfo_Dict[x.name] = {"name":x.name, "symbol": x.symbol, "id": x.id, "rank":x.rank}
    jsonresponse = [ v for k,v in sorted(BasicInfo_Dict.items(), key=lambda x:x[1]['rank']) ]

    return jsonify(jsonresponse[:no_variables])


@app.route('/api/priceinfo/<string:coinid>/')
def get_priceinfo(coinid):
    no_rows = 25*12
    query = ("""
        SELECT id, time, price_usd, volume_usd_24h
        FROM priceinfo
        WHERE id='{Coin_Id}'
        LIMIT {Limit};
    """).format(Coin_Id=coinid, Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    jsonresponse = [{'id': x.id, 'time': x.time.isoformat(), 'price_usd': x.price_usd, \
                    'volume_usd_24h': x.volume_usd_24h} for x in response]
    return jsonify(jsonresponse)


@app.route('/api/priceinfo/<string:coinid>/<string:start_time>/<string:end_time>/')
def get_priceinfo_timeperiod(coinid, start_time, end_time):
    query = ("""
        SELECT id, time, price_usd, volume_usd_24h
        FROM priceinfo
        WHERE id='{Coin_Id}'
            AND time > {Start_Time}
            AND time < {End_Time}
        ALLOW FILTERING;
    """).format(Coin_Id=coinid, Start_Time=start_time, End_Time=end_time)\
        .translate(None, '\n')
    print query
    response = session.execute(query)

    jsonresponse = [{'id': x.id, 'time': x.time.isoformat(), 'price_usd': x.price_usd, \
                    'volume_usd_24h': x.volume_usd_24h} for x in response]
    return jsonify(jsonresponse)






@app.route('/api/correlation/<string:coinid_a>/<string:coinid_b>/')
def get_corr_coins(coinid_a, coinid_b):
    no_rows = 1
    query = ("""
        SELECT date, time, corr
        FROM priceinfocorr
        WHERE date='{Date}'
        LIMIT {Limit};
    """).format(Date=time.strftime("%d-%m-%Y"), Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    resp_list = [{'time': x.time.isoformat(), \
                  'corr_matrix': ast.literal_eval(x.corr)} for x in response]

    resp_dict = resp_list[0]
    resp_dict['corr'] = resp_dict['corr_matrix'][ ID_DICT[coinid_a] ][ ID_DICT[coinid_b] ]


    jsonresponse = [{'time': resp_dict['time'], \
                    'corr': resp_dict['corr']} ]

    return jsonify(jsonresponse)



@app.route('/api/correlation/lastest_matrix')
def get_corr_latest():
    no_rows = 1
    query = ("""
        SELECT date, time, corr
        FROM priceinfocorr
        WHERE date='{Date}'
        LIMIT {Limit};
    """).format(Date=time.strftime("%d-%m-%Y"), Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    resp_list = [{'time': x.time.isoformat(), \
                  'corr_matrix': ast.literal_eval(x.corr)} for x in response]

    resp_dict = resp_list[0]
    resp_dict['corr'] = [ row[ :10] for row in resp_dict['corr_matrix'][ :10] ]


    jsonresponse = [{'time': resp_dict['time'], \
                    'corr': resp_dict['corr']} ]

    return jsonify(jsonresponse)
