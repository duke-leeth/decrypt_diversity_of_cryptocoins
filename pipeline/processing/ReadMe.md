# Processing

## Start processing process
Using the following command to start the processing process
```
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host=<IP address> consumer.py &
```
