# Stress Test

## Start producer process
Using the following command to start the producer process,
and store the log message into `producer_testing_log.txt` file
```
nohup python insight_project/stress_test/producer_testing.py &>producer_testing_log.txt &
```


## Start processing process
Using the following command to start the processing process
```
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host= <IP address> consumer_testing.py &
```
