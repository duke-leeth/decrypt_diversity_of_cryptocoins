# Source


## Create topic
ssh into master node, create topic via the following command,
```
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 12 --topic CoinsInfo
```

check and see if this topic is seen by other nodes by describing them on another node,
```
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic CoinsInfo
```

## Start producer process
Using the following command to start the producer process,
and store the log message into `producer_log.txt` file
```
nohup python insight_project/pipeline/ingestion/producer.py &>producer_log.txt &
```
