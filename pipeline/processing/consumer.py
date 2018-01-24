# -*- coding: utf-8 -*-
#

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = \
'--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import sys
import time
import json
import threading
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import config

sc = SparkContext(appName='Processing_CoinsInfo')
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 10)

ZK = config.INGESTION_CONFIG['ZK_PUBLIC_DNS']

GRIUP_ID = 'spark-streaming'
TOPIC = 'CoinsInfo'
NO_PARTITION = 10

kafkaStream = KafkaUtils.createStream(ssc, ZK, GRIUP_ID, {TOPIC : NO_PARTITION})

kafkaStream.map(lambda entry: print json.loads(entry))

ssc.start()
ssc.awaitTermination()
