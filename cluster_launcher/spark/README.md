# Spark

## Install command `bc` on each node (including master node)
1. Update `apt-get`
```
sudo apt-get update
```

2. Install `bc`
```
sudo apt-get install bc
```


## Install `Spark` on cluster via pegasus
```
peg install ${CLUSTER_NAME} spark
```


## Set the memory size
1. ssh into each node,
```
peg ssh spark-cluster <no.>
```

2. Edit the `spark-env.sh` file via the following command,
```
vim /usr/local/spark/conf/spark-env.sh
```

3. Add the following
```
export SPARK_EXECUTOR_MEMORY=6G
```

4. Edit the `spark-defaults.conf` file via the following command,
```
vim /usr/local/spark/conf/spark-defaults.conf
```

5. Add the following
```
spark.driver.memory 2g
```


## Start Spark
```
peg service spark-cluster spark start
```


## Reference
* [Insight-Spark](https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/spark-intro)






# Hadoop

## Start Hadoop
**IMPORTANT!!!** Make sure `bc` has been install, please check the ReadMe_spark.md for more detail

```
peg service spark-cluster hadoop start
```


## Create a HDFS directory for spark sliding window library
1. ssh into HDFS namenode
```
peg ssh spark-cluster 1
```

2. Create a HDFS checkpoint directory
```
hdfs dfs -mkdir /checkpoint
```

3. List directories from top level of HDFS
```
hdfs dfs -ls /
```
