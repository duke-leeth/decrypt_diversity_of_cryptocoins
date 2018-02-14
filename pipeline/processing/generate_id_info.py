# -*- coding: utf-8 -*-
#

from cassandra.cluster import Cluster
import sys
import config


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptocoins'
TABLE_NAME = 'basicinfo'

ID_LIST_FILE = 'id_info.py'


def connect_to_cassandra(cassandra_dns=CASSANDRA_DNS, keyspace=KEYSPACE):
    """ Connect to cassandra given keyspace

        Args:   <cassandra_dns: String>
                <keyspace: String>
        Return: <an object of class cassandra.cluster.Session>
    """
    cluster = Cluster([cassandra_dns])
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session


def get_id_info(session, keyspace=KEYSPACE, table_name=TABLE_NAME, id_list_file=ID_LIST_FILE):
    """ Get ranking information of all coins from cassandra, and save them in a .py file

        Args:   <session: an object of class cassandra.cluster.Session>
                <keyspace: String>
                <table_name: String>
                <id_list_file: String>
        Return: Void
    """
    query = ("""SELECT id, rank FROM {Keyspace}.{Table_Name};""")\
            .format(Keyspace=keyspace , Table_Name = table_name)
    response = session.execute(query)

    id_dict = {}
    for row in response:
        id_dict[row.id] = row.rank

    inv_id_dict = {int(v): k for k, v in id_dict.iteritems()}
    id_list = [ k for k,v in sorted(id_dict.items(), key=lambda x:int(x[1])) ]

    with open(id_list_file, 'w') as fout:
        fout.write('ID_DICT = ')
        fout.write(str(id_dict).encode("UTF-8"))
        fout.write('\n\n')

        fout.write('INV_ID_DICT = ')
        fout.write(str(inv_id_dict).encode("UTF-8"))
        fout.write('\n\n')

        fout.write('ID_LIST = ')
        fout.write(str(id_list).encode("UTF-8"))
        fout.write('\n\n')


def main(argv=sys.argv):
    """ Get ranking information of all coins from cassandra, and save them in a .py file

        Return: Void
    """
    session = connect_to_cassandra(CASSANDRA_DNS, KEYSPACE)
    get_id_info(session, KEYSPACE, TABLE_NAME, ID_LIST_FILE)



if __name__ == '__main__':
    main()
