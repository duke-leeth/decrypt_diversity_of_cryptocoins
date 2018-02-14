# -*- coding: utf-8 -*-
#

import sys
import requests
import json
from cassandra.cluster import Cluster
import config

PUBLIC_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
API_URL = config.COIN_SOURCE_CONFIG['API_URL']

KEYSPACE = 'cryptocoins'
TABLE_NAME = 'basicinfo'


def set_keyspace(session, keyspace=KEYSPACE):
    """ Create or set the keyspace for a session object

        Args:   <session: an object of class cassandra.cluster.Session>
                <keyspace: String>
        Return: Void
    """
    replication_setting = \
        '{\'class\' : \'SimpleStrategy\', \'replication_factor\' : 3}'
    session.execute( \
        ("""
            CREATE KEYSPACE IF NOT EXISTS {Keyspace}
            WITH REPLICATION = {Replication_Setting};
        """).format(Keyspace = keyspace, Replication_Setting = replication_setting)\
        .translate(None, '\n') \
    )
    session.set_keyspace(keyspace)


def create_table(session, table_name=TABLE_NAME):
    """ Create a table for basic coins information

        Args:   <session: an object of class cassandra.cluster.Session>
                <table_name: String>
        Return: Void
    """
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
    """ Return a query object for cassandra insertion preparation

        Args:   <session: an object of class cassandra.cluster.Session>
                <table_name: String>
        Return: <an object of class cassandra.query.PreparedStatement>
    """
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
    """ Send an API request to get basic information of all coins,
        and save them into cassandra

        Args:   <session: an object of class cassandra.cluster.Session>
                <table_name: String>
        Return: Void
    """
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



def main(argv=sys.argv):
    """ Get basic information of all coins and insert them into Cassandra

        Return: Void
    """
    cluster = Cluster([PUBLIC_DNS])
    session = cluster.connect()
    set_keyspace(session, KEYSPACE)
    create_table(session, TABLE_NAME)
    send_request(session, TABLE_NAME)



if __name__ == '__main__':
    main()
