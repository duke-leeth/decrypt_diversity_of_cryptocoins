# -*- coding: utf-8 -*-
#


import sys
import time
import requests
import json
import kafka
import config
import id_info
import random

ID_DICT = id_info.ID_DICT
INV_ID_DICT = id_info.INV_ID_DICT
ID_LIST = id_info.ID_LIST

PUBLIC_DNS = config.INGESTION_CONFIG['KAFKA_PUBLIC_DNS']
TOPIC = 'CoinsInfo'
API_URL = config.COIN_SOURCE_CONFIG['API_URL']

RECORDS_LIST_NAME = 'records_list'
TIME_PERIOD = 0.1
NO_OF_COINS = 1000


def cast_to_float(var):
    """ Casts the input variable into float

        Args:   <var: String>
        Return: Float if can be cast else 0.0
    """
    try:
        var = float(var)
        return var
    except Exception as ex:
        return 0.0


def send_request():
    """ Sends a http API request to get the realtime data,
        cleans it, and sends it to Kafka
        Return: Void
    """

    # Send http API request
    try:
        req = requests.get(API_URL)
        jsdata = json.loads(req.text)
    except Exception as ex:
        return None


    curr_time = int(time.time()*1000)
    records_list = [ {} for i in range(NO_OF_COINS) ]

    # Put the valid data points into a list
    for entry in jsdata:
        if entry['id'] in ID_DICT:
            index = ID_DICT[entry['id']] - 1
            if index < NO_OF_COINS:
                entry['time'] = curr_time
                entry['price_usd'] = cast_to_float(entry['price_usd'])
                entry['price_btc'] = cast_to_float(entry['price_btc'])
                entry['volume_usd_24h'] = cast_to_float(entry['24h_volume_usd'])
                entry['market_cap_usd'] = cast_to_float(entry['market_cap_usd'])
                entry['available_supply'] = cast_to_float(entry['available_supply'])
                entry['total_supply'] = cast_to_float(entry['total_supply'])
                entry['max_supply'] = cast_to_float(entry['max_supply'])
                entry['percent_change_1h'] = cast_to_float(entry['percent_change_1h'])
                entry['percent_change_24h'] = cast_to_float(entry['percent_change_24h'])
                entry['percent_change_7d'] = cast_to_float(entry['percent_change_7d'])

                records_list[index] = entry

    # Fill in missing data points
    for i in range(len(records_list)):
        if len(records_list[i]) == 0:
            entry = records_list[i]
            entry['id'] = ID_LIST[i]
            entry['time'] = curr_time
            entry['price_usd'] = 0.0
            entry['price_btc'] = 0.0
            entry['volume_usd_24h'] = 0.0
            entry['market_cap_usd'] = 0.0
            entry['available_supply'] = 0.0
            entry['total_supply'] = 0.0
            entry['max_supply'] = 0.0
            entry['percent_change_1h'] = 0.0
            entry['percent_change_24h'] = 0.0
            entry['percent_change_7d'] = 0.0

    return records_list


def simulate_data_and_send(records_list):
    percent = random.uniform(0.0, 0.20)

    for i in range(len(records_list)):
        mean = records_list[i]['price_usd']
        records_list[i]['price_usd'] = random.normalvariate(mean, mean*percent)

    return records_list


def periodic_request(producer, time_period=10):
    """ Periodically executes API request and sends the clean data to Kafka

        Args:   <producer: producer object from kafka.KafkaProducer>
                <time_period (sec): float>
        Return: Void
    """

    records_list = send_request()

    while True:
        records_list = simulate_data_and_send(records_list)

        # Send to kafka
        producer.send(TOPIC, json.dumps({RECORDS_LIST_NAME:records_list}))
        time.sleep(time_period)


def main(argv=sys.argv):
    """ Produce data to the pipline

        Return: Void
    """
    producer = kafka.KafkaProducer(bootstrap_servers=PUBLIC_DNS)
    periodic_request(producer, TIME_PERIOD)



if __name__ == '__main__':
    main()
