# -*- coding: utf-8 -*-
#

import os
import sys
import time
import json
import threading
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from cassandra.cluster import Cluster

import config
import id_dict


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptcoin'
TABLE_NAME = 'priceinfo'
TABLE_NAME_CORR = 'pricecorr'

ID_DICT = id_dict.ID_DICT
INV_ID_DICT = {int(v): k for k, v in ID_DICT.iteritems()}
ID_LIST = [ k for k,v in sorted(ID_DICT.items(), key=lambda x:int(x[1])) ]

RECORD_LIMIT = 12 
TIME_PERIOD = 15*60


def connect_to_cassandra(cassandra_dns=CASSANDRA_DNS, keyspace=KEYSPACE):
    cluster = Cluster([cassandra_dns])
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session



def create_table_corr(session, table_name=TABLE_NAME_CORR):
    query = ("""
        CREATE TABLE IF NOT EXISTS {Table_Name} (
            id_a text,
            id_b text,
            time timestamp,
            corr float,
            PRIMARY KEY ((id_a, id_b), time),
        ) WITH CLUSTERING ORDER BY (time DESC);
    """).format(Table_Name = table_name).translate(None, '\n')
    table_creation_preparation = session.prepare(query)
    session.execute(table_creation_preparation)


def prepare_insertion_corr(session, table_name=TABLE_NAME_CORR):
    query = ("""
        INSERT INTO {Table_Name} (
            id_a,
            id_b,
            time,
            corr
        ) VALUES (?,?,?,?)
    """).format(Table_Name = table_name).translate(None, '\n')
    return session.prepare(query)


def query_priceinfo(session, coinid):
    table_name = TABLE_NAME
    record_limit = RECORD_LIMIT
    query = ("""
        SELECT price_usd
        FROM {Table_Name}
        WHERE id='{Id}'
        LIMIT {Record_Limit};
    """).format(Table_Name=table_name, \
                Id=coinid, Record_Limit=record_limit).translate(None, '\n')
    
    response = session.execute(query)
    return [x.price_usd for x in response]


def compute_correlation(session, curr_time):
    record_limit = RECORD_LIMIT
    list_matrix = []
    no_of_variable = 950

    for i in range(no_of_variable):
        price_list = query_priceinfo(session, ID_LIST[i])
        if len(price_list) != record_limit:
            for i in range(record_limit - len(price_list)):
                price_list.append(0)
        list_matrix.append( price_list )
        price_list = []


    corr_matrix = np.corrcoef( np.matrix(list_matrix) )
    corr_matrix = np.nan_to_num( corr_matrix )

    query_cassandra = prepare_insertion_corr(session, TABLE_NAME_CORR)
    for i in range(len(corr_matrix)):
        for j in range(len(corr_matrix)):
            session.execute(query_cassandra,\
                (INV_ID_DICT[i+1], INV_ID_DICT[j+1], \
                 curr_time, corr_matrix[i][j]) )


def periodic_compute(session, time_period=TIME_PERIOD):
    if not isinstance(time_period, int):
	raise ValueError('time_period must be an integer.')

    while True:
        compute_correlation(session, int(time.time()*1000) )
        time.sleep(time_period)



def main(argv=sys.argv):
    session = connect_to_cassandra(CASSANDRA_DNS, KEYSPACE)
    create_table_corr(session, TABLE_NAME_CORR)
    periodic_compute(session, TIME_PERIOD)




if __name__ == "__main__":
    main()
