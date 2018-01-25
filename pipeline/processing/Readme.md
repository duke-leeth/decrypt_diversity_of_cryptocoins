# Processing


## Start processing process
Using the following command to start the processing process
```
/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 consumer.py
```



/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host=ec2-34-211-208-8.us-west-2.compute.amazonaws.com consumer.py
