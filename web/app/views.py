# -*- coding: utf-8 -*-
#
# TIME format: rfc3339
import time

from flask import jsonify

from flask import render_template
from app import app

from cassandra.cluster import Cluster

import config
import id_info



CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptcoin'

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
    query = 'SELECT name, symbol, id, rank FROM cryptcoin.basicinfo;'
    response = session.execute(query)
    response_list = []
    for val in response:
        response_list.append(val)

    BasicInfo_Dict = {}
    for x in response_list:
        BasicInfo_Dict[x.name] = {"name":x.name, "symbol": x.symbol, "id": x.id, "rank":x.rank}
    jsonresponse = [ v for k,v in sorted(BasicInfo_Dict.items(), key=lambda x:x[1]['rank']) ]

    return jsonify(jsonresponse)


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
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE id_a='{Coin_Id_A}' AND id_b='{Coin_Id_B}'
        LIMIT {Limit};
    """).format(Coin_Id_A=coinid_a, Coin_Id_B=coinid_b, Limit=no_rows).translate(None, '\n')
    response = session.execute(query)

    jsonresponse = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                    'corr': x.corr} for x in response]
    return jsonify(jsonresponse)


@app.route('/api/correlation/<string:coinid_a>/<string:coinid_b>/<string:start_time>/<string:end_time>/')
def get_corr_coins_timeperiod(coinid_a, coinid_b, start_time, end_time):
    query = ("""
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE id_a='{Coin_Id_A}'
            AND id_b='{Coin_Id_B}'
            AND time > {Start_Time}
            AND time < {End_Time}
        ALLOW FILTERING;
    """).format(Coin_Id_A=coinid_a, Coin_Id_B=coinid_b, \
                Start_Time=start_time, End_Time=end_time) \
        .translate(None, '\n')
    response = session.execute(query)

    jsonresponse = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                    'corr': x.corr} for x in response]
    return jsonify(jsonresponse)



@app.route('/api/correlation/lastest_matrix')
def get_corr_latest():
    start_time = int(time.time()*1000) - 28*60*1000
    end_time = int(time.time()*1000)

    no_rows = 1500
    query = ("""
        SELECT id_a, id_b, time, corr
        FROM pricecorr
        WHERE time > {Start_Time} AND time < {End_Time}
        LIMIT {Limit}
        ALLOW FILTERING;
    """).format(Start_Time=start_time, End_Time=end_time, Limit=no_rows) \
        .translate(None, '\n')

    def query_each(id_a, id_b):
        query = ("""
            SELECT id_a, id_b, time, corr
            FROM pricecorr
            WHERE id_a='{Id_A}' AND id_b='{Id_B}'
            LIMIT {Limit}
            ALLOW FILTERING;
        """).format(Id_A=id_a, Id_B=id_b, Limit=1).translate(None, '\n')

        response = session.execute(query)
        jsdata_array = [{'id_a': x.id_a, 'id_b': x.id_b, 'time': x.time.isoformat(), \
                        'corr': x.corr} for x in response]
        return jsdata_array[0]


    no_of_variable = 10
    corr_matrix = [ [0 for i in range(no_of_variable)] for i in range(no_of_variable) ]

    for i in range(len(ID_LIST[:no_of_variable])):
        for j in range(len(ID_LIST[:no_of_variable])):
            corr_matrix[i][j] = query_each(ID_LIST[i], ID_LIST[j])['corr']

    jsonresponse = {'corr_matrix': corr_matrix, 'tag_list': ID_LIST[:no_of_variable]}
    return jsonify(jsonresponse)
