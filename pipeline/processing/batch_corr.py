# -*- coding: utf-8 -*-
#

import os
import sys
import time
import json
import threading
import pyspark_cassandra
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
INV_ID_DICT = {v: k for k, v in ID_DICT.iteritems()}
ID_LIST = { k for k,v in sorted(ID_DICT.items(), key=lambda x:x[1]) }

TIME_PERIOD = 30


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
    RecordLimit = 12
    query = ("""
        SELECT price_usd
        FROM {Table_Name}
        WHERE id={Id}
        LIMIT {Limit};
    """).format(Table_Name=table_name, \
                Id=coinid, Limit=RecordLimit).translate(None, '\n')
    response = session.execute(query)
    return [x.price_usd for x in response]


def compute_correlation(session):
    list_matrix = []
    for i in range(len(ID_LIST)):
        list_matrix.append( query_priceinfo(session, ID_LIST[i]) )

    corr_matrix = np.corrcoef( np.matrix(list_matrix) )
    corr_matrix = np.nan_to_num( corr_matrix )


    query_cassandra = prepare_insertion_corr(session, TABLE_NAME_CORR)
    for i in range(len(corr_matrix)):
        for j in range(len(corr_matrix)):
            session.execute(query_cassandra,\
                (INV_ID_DICT[i+1], INV_ID_DICT[j+1], \
                 curr_time, corr_matrix[i][j]) )


    print corr_matrix.shape
    print corr_matrix[:3][:2]


def periodic_compute(session, time_period=TIME_PERIOD):
    if not isinstance(time_period, int):
	raise ValueError('time_period must be an integer.')

    while True:
        compute_correlation(session)
        time.sleep(time_period)



def main(argv=sys.argv):
    session = connect_to_cassandra(CASSANDRA_DNS, KEYSPACE)
    create_table_corr(session, TABLE_NAME_CORR)
    periodic_compute(session, TIME_PERIOD)




if __name__ == "__main__":
    main()
