# Cassandra

## Installation
After using Pegasus for peg up all nodes for a cluster,

## Open AWS `Security Groups` tag
**IMPORTANT!!!** allows All Traffic to Internal Security Group which all the nodes belong to


## Start Cassandra
1. ssh into each node, including master node and all worker nodes
```
peg ssh cassandra-cluster <no.>
```
2. Start cassandra service
```
cassandra
```


## Monitoring and managing
1. ssh into master node
```
peg ssh cassandra-cluster 1
```

2. Use the following command,
```
nodetool status
```


## Reference
* [Insight-Cassandra](https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/cassandra)
* [Python-Driver](https://github.com/datastax/python-driver)
* [Cassandra Driver 3.12.0 doc](http://datastax.github.io/python-driver/index.html)
