# Kafka

## Installation
After using Pegasus for peg up all nodes for a cluster,


## Open AWS `Security Groups` tag
**IMPORTANT!!!** allows All Traffic to Internal Security Group which all the nodes belong to


## Start Kafka

### Launch environment for Kafka
1. ssh into master node
```
peg ssh kafka-cluster 1
```

2. Start kafka-manager service
```
sudo $KAFKA_MANAGER_HOME/bin/kafka-manager -Dhttp.port=9001 &
```

3. Start zookeeper service
```
sudo /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
```

### Start Kafka service
1. ssh into each node, including master node and all worker nodes
```
peg ssh kafka-cluster <no.>
```

2. Start kafka service on each node
```
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
```


## Reference
* [Insight-Kafka](https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/Kafka#check-kafka-configuration)
* [Kafka Quickstart](https://kafka.apache.org/quickstart)
