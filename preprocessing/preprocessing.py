# -*- coding: utf-8 -*-

import sys
import requests
import json
from cassandra.cluster import Cluster
import config

PUBLIC_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
API_URL = config.COIN_SOURCE_CONFIG['API_URL']

KEYSPACE = 'cryptcoin'
TABLE_NAME = 'basicinfo'

ID_LIST_FILE = 'id_dict.py'


def set_keyspace(session, keyspace=KEYSPACE):
    replication_setting = \
        '{\'class\' : \'SimpleStrategy\', \'replication_factor\' : 3}'
    session.execute( \
        ("""
        CREATE KEYSPACE IF NOT EXISTS {KeySpace}
         WITH REPLICATION = {Replication_Setting};
    """).format(KeySpace = keyspace, \
                Replication_Setting = replication_setting).translate(None, '\n') \
    )
    session.set_keyspace(keyspace)


def create_table(session, table_name=TABLE_NAME):
    query = ("""
        CREATE TABLE IF NOT EXISTS {Table_Name} (
            id text,
            name text,
            symbol text,
            rank int,
            PRIMARY KEY ((id), rank),
        ) WITH CLUSTERING ORDER BY (rank ASC);
    """).format(Table_Name = table_name).translate(None, '\n')
    table_creation_preparation = session.prepare(query)
    session.execute(table_creation_preparation)


def prepare_insertion(session, table_name=TABLE_NAME):
    query = ("""
        INSERT INTO {Table_Name} (
            id,
            name,
            symbol,
            rank
        ) VALUES (?,?,?,?)
    """).format(Table_Name = table_name).translate(None, '\n')
    return session.prepare(query)


def send_request(session, table_name=TABLE_NAME):
    while True:
        try:
            req = requests.get(API_URL)
            jsdata = json.loads(req.text)
            break
        except Exception as ex:
            pass

    query_cassandra = prepare_insertion(session, table_name)

    id_dict = {} 

    for entry in jsdata:
        id_dict[ entry['id'] ] = entry['rank']

        session.execute(query_cassandra, \
                    (entry['id'], entry['name'], entry['symbol'], int(entry['rank'])))

    with open(ID_LIST_FILE, 'w') as fout:
        fout.write('ID_DICT = ')
        fout.write(str(id_dict).encode("UTF-8"))



def main(argv=sys.argv):
    cluster = Cluster([PUBLIC_DNS])
    session = cluster.connect()
    set_keyspace(session, KEYSPACE)
    create_table(session, TABLE_NAME)
    send_request(session, TABLE_NAME)



if __name__ == '__main__':
    main()
