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
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

import config
import id_dict


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptcoin'
TABLE_NAME = 'priceinfo'
TABLE_NAME_HOURLY = 'priceinfohourly'
TABLE_NAME_CORR = 'pricecorr'

ZK_DNS = config.INGESTION_CONFIG['ZK_PUBLIC_DNS']
BATCH_DURATION = 10

GRIUP_ID = 'spark-streaming'
TOPIC = 'CoinsInfo'
NO_PARTITION = 12

APP_NAME = 'processing'
MASTER = config.PROCESSING_CONFIG['PUBLIC_DNS']
PATH_CHECKPOINT = config.PROCESSING_CONFIG['HDFS']


WINDOW_LENGTH = 5*60
SLIDE_INTERVAL = 5*60


ID_DICT = id_dict.ID_DICT
NO_COINS = 1506

INV_ID_DICT = {v: k for k, v in ID_DICT.iteritems()}



def connect_to_cassandra(cassandra_dns=CASSANDRA_DNS, keyspace=KEYSPACE):
    cluster = Cluster([cassandra_dns])
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session


def create_table(session, table_name=TABLE_NAME):
    query = ("""
        CREATE TABLE IF NOT EXISTS {Table_Name} (
            id text,
            time timestamp,
            price_usd float,
            price_btc float,
            volume_usd_24h float,
            market_cap_usd float,
            available_supply float,
            total_supply float,
            max_supply float,
            percent_change_1h float,
            percent_change_24h float,
            percent_change_7d float,
            PRIMARY KEY ((id), time),
        ) WITH CLUSTERING ORDER BY (time DESC);
    """).format(Table_Name = table_name).translate(None, '\n')
    table_creation_preparation = session.prepare(query)
    session.execute(table_creation_preparation)


def create_table_hourly(session, table_name=TABLE_NAME_HOURLY):
    query = ("""
        CREATE TABLE IF NOT EXISTS {Table_Name} (
            id text,
            time timestamp,
            price_usd float,
            price_btc float,
            PRIMARY KEY ((id), time),
        ) WITH CLUSTERING ORDER BY (time DESC);
    """).format(Table_Name = table_name).translate(None, '\n')
    table_creation_preparation = session.prepare(query)
    session.execute(table_creation_preparation)


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


def sum_and_count(entry):
    return ( entry['id'], \
             {'count': 1, \
              'price_usd': entry['price_usd'], \
              'price_btc': entry['price_btc']} )


def addition(v1, v2):
    return {'count': v1['count'] + v2['count'], \
            'price_usd': v1['price_usd'] + v2['price_usd'], \
            'price_btc': v1['price_btc'] + v2['price_btc'] }


def subtraction(v1, v2):
    return {'count': v1['count'] - v2['count'], \
            'price_usd': v1['price_usd'] - v2['price_usd'], \
            'price_btc': v1['price_btc'] - v2['price_btc'] }


def average_price((key, value)): \
    return {'id': key, \
            'time': int(time.time()*1000), \
            'price_usd': value['price_usd']/value['count'], \
            'price_btc': value['price_btc']/value['count'] }


def list_append(v1, v2):
    return v1 + v2


def process_corr(dstream, session):
    query_cassandra = prepare_insertion_corr(session, TABLE_NAME_CORR)
    
    def compute_correlation(rdd):
        list_matrix = []
        for i in range(NO_COINS):
            list_matrix.append([])

        info_list = []
        try:
            info_list = rdd.collect()
        except Exception as ex:
            pass

        for (k, v) in info_list:
            try:
                list_matrix[ int( ID_DICT[k] ) - 1 ] = v
            except Exception as ex:
                pass

        no_of_var = 250

        corr_matrix = np.corrcoef( np.matrix(list_matrix[:no_of_var]) )
        corr_matrix = np.nan_to_num( corr_matrix )
        
        print corr_matrix.shape
        print corr_matrix[:3][:2]

        curr_time = int(time.time()*1000)

        for i in range(no_of_var):
            for j in range(no_of_var):
                session.execute(query_cassandra,\
                    (INV_ID_DICT[i+1], \
                     INV_ID_DICT[j+1], \
                     curr_time, \
                     corr_matrix[i][j]) )

        return None


    dstream.foreachRDD(compute_correlation)



def main(argv=sys.argv):

    spark = SparkSession.builder.appName(APP_NAME).master(MASTER).getOrCreate()
    sc = spark.sparkContext

    sc.setLogLevel("WARN")
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, BATCH_DURATION)

    ssc.checkpoint(PATH_CHECKPOINT)

    session = connect_to_cassandra(CASSANDRA_DNS, KEYSPACE)
    create_table(session, TABLE_NAME)
    create_table_hourly(session, TABLE_NAME_HOURLY)
    create_table_corr(session, TABLE_NAME_CORR)

    kafkaStream = KafkaUtils.createStream(ssc, ZK_DNS, GRIUP_ID, {TOPIC : NO_PARTITION})
    decodedStream = kafkaStream.map(lambda (time, records): json.loads(records))
#    decodedStream.foreachRDD(lambda rdd: rdd.saveToCassandra(KEYSPACE, TABLE_NAME))

#    hourlyStream = decodedStream\
#                    .map(sum_and_count)\
#                    .reduceByKeyAndWindow(addition, subtraction, WINDOW_LENGTH, SLIDE_INTERVAL )

#    hourlyStream.map(average_price)\
#                .foreachRDD(lambda rdd: rdd.saveToCassandra(KEYSPACE, TABLE_NAME_HOURLY))

    

    culStream = decodedStream\
                    .map(lambda entry: (entry['id'], [ entry['price_usd'] ]) ) \
                    .reduceByKeyAndWindow(list_append, WINDOW_LENGTH, SLIDE_INTERVAL)
    
    process_corr(culStream, session)
#    culStream.foreachRDD(compute_correlation)




    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
