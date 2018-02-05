# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import sys
import config


CASSANDRA_DNS = config.STORAGE_CONFIG['PUBLIC_DNS']
KEYSPACE = 'cryptcoin'

cluster = Cluster([CASSANDRA_DNS])
session = cluster.connect()
session.set_keyspace(KEYSPACE)

ID_LIST_FILE = 'id_info.py'


def get_id_info():
    query = "SELECT id, rank FROM cryptcoin.basicinfo;"
    response = session.execute(query)

    id_dict = {}
    for row in response:
        id_dict[row.id] = row.rank

    inv_id_dict = {int(v): k for k, v in id_dict.iteritems()}
    id_list = [ k for k,v in sorted(id_dict.items(), key=lambda x:int(x[1])) ]

    with open(ID_LIST_FILE, 'w') as fout:
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
    get_id_info()



if __name__ == '__main__':
    main()
