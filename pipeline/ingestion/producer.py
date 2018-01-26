# -*- coding: utf-8 -*-

import sys
import time
import requests
import json
import jsonlines
import kafka
import config


PUBLIC_DNS = config.INGESTION_CONFIG['PUBLIC_DNS']
TOPIC = 'CoinsInfo'
API_URL = config.COIN_SOURCE_CONFIG['API_URL']
TIME_PERIOD = 10

def cast_to_float(var):
    try:
        var = float(var)
        return var
    except Exception as ex:
        return 0.0


def send_request(producer):
    try:
        req = requests.get(API_URL)
        jsdata = json.loads(req.text)
    except Exception as ex:
        return None

    for entry in jsdata:
        entry['time'] = int(cast_to_float(entry['last_updated']) * 1000)
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

        producer.send(TOPIC, json.dumps(entry))


def periodic_request(producer, time_period=10):
    if not isinstance(time_period, int):
	raise ValueError('time_period must be an integer.')

    while True:
        send_request(producer)
        producer.flush()
        time.sleep(time_period)


def main(argv=sys.argv):
    producer = kafka.KafkaProducer(bootstrap_servers=PUBLIC_DNS)
    periodic_request(producer, TIME_PERIOD)



if __name__ == '__main__':
    main()
