# -*- coding: utf-8 -*-
#

import os
import sys
import time
import json
import threading
import pyspark_cassandra
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import numpy as np

import config
import id_info

ID_DICT = id_info.ID_DICT
INV_ID_DICT = id_info.INV_ID_DICT
ID_LIST = id_info.ID_LIST

CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptocoins'
TABLE_NAME = 'priceinfo'
TABLE_NAME_HOURLY = 'priceinfohourly'
TABLE_NAME_CORR = 'priceinfocorr'

ZK_DNS = config.INGESTION_CONFIG['ZK_PUBLIC_DNS']
BATCH_DURATION = 10

GRIUP_ID = 'spark-streaming'
TOPIC = 'CoinsInfo'
NO_PARTITION = 12

APP_NAME = 'streaming_processing'
MASTER = config.PROCESSING_CONFIG['PUBLIC_DNS']
PATH_CHECKPOINT = config.PROCESSING_CONFIG['HDFS']

RECORDS_LIST_NAME = 'records_list'

WINDOW_LENGTH = 60*60
SLIDE_INTERVAL = 60*60

WINDOW_LENGTH_CORR = 60*60
SLIDE_INTERVAL_CORR = 6*60


SPARK = SparkSession.builder.appName(APP_NAME).master(MASTER).getOrCreate()


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
            date text,
            time timestamp,
            corr text,
            PRIMARY KEY ((date), time),
        ) WITH CLUSTERING ORDER BY (time DESC);
    """).format(Table_Name = table_name).translate(None, '\n')
    table_creation_preparation = session.prepare(query)
    session.execute(table_creation_preparation)


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


def to_corr_list(records_list):
    return [ [ entry['price_usd'] ] for entry in records_list ]



def union(l1, l2):
    corr_matrix = []

    for s1,s2 in zip(l1,l2):
        corr_matrix.append( s1+s2 )

    return corr_matrix


def minus(l1, l2): 
    corr_matrix = []
    
    for i in range(len(l1)):
        corr_matrix.append( l1[i][len(l2)-1:]  )
    
    return corr_matrix


def correlation(matrix):
    corr_matrix = np.corrcoef( np.matrix(matrix) )
    corr_matrix = np.nan_to_num( corr_matrix ) 

    return corr_matrix


def corr_to_db_format(corr_matrix):
    return {'date': time.strftime("%d-%m-%Y"), \
            'time': int(time.time()*1000), \
            'corr': str(corr_matrix) }


def main(argv=sys.argv):

    # Initialize Spark
    spark = SPARK
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, BATCH_DURATION)

    # Setting a HDFS checkpoint path for Spark
    ssc.checkpoint(PATH_CHECKPOINT)

    # Connect to Cassandra
    session = connect_to_cassandra(CASSANDRA_DNS, KEYSPACE)

    # Create tables in Cassandra
    create_table(session, TABLE_NAME)
    create_table_hourly(session, TABLE_NAME_HOURLY)
    create_table_corr(session, TABLE_NAME_CORR)


    # Get streaming data from Kafka
    kafkaStream = KafkaUtils.createStream(ssc, ZK_DNS, GRIUP_ID, {TOPIC : NO_PARTITION})
    listDataStream = kafkaStream.map(lambda (time, records): json.loads(records))\
                            .map(lambda x: x[RECORDS_LIST_NAME])\

    # Save streaming data to Cassandra
    decodedStream = listDataStream.flatMap(lambda records_list: records_list)
    decodedStream.foreachRDD(lambda rdd: rdd.saveToCassandra(KEYSPACE, TABLE_NAME))



    # Compute mean hourly price for each coin via sliding window
    hourlyStream = decodedStream\
                   .map(sum_and_count)\
                   .reduceByKeyAndWindow(addition, subtraction, WINDOW_LENGTH, SLIDE_INTERVAL)

   # Save hourly streaming data to Cassandra
    hourlyStream.map(average_price)\
                .foreachRDD(lambda rdd: rdd.saveToCassandra(KEYSPACE, TABLE_NAME_HOURLY))



    # Compute correlation matrix
    corrStream = listDataStream.map(to_corr_list) \
                               .reduceByWindow(union, minus, WINDOW_LENGTH_CORR, SLIDE_INTERVAL_CORR)

    # Save correlation matrix to cassandra
    corrStream.map(correlation)\
              .map(corr_to_db_format)\
              .foreachRDD(lambda rdd: rdd.saveToCassandra(KEYSPACE, TABLE_NAME_CORR))


    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
