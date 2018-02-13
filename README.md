# Decrypt Diversity of Cryptocoins

## Summary
A streaming data pipeline to show the correlation matrix (the latest one hour) as heatmap for cryptocoins. This project was built during being as a Data Engineering Fellow at Insight Data Science.

* [Slides](http://www.bit.ly/2ntauKR)
* [Platform](http://www.bit.ly/2s3wlxo)


## Project idea in 1-sentence
To provide a real-time risk management system in cryptocurrency market.

## Purpose and Use cases
### Purpose
Whenever people invest in financial market, they want to reduce the risk by diversifying their portfolio.
Otherwise, they may become either very rich, or very poor. For example, someone invests money into two coins, coin A and coin B. If price A and price B are highly correlated, which means that their prices may go high or drop dramatically together, then that person will become very rich, or extremely poor. That is something people are not looking forward to.

The metric for this case is the correlations between all the investment targets. However, unlike the stock market, in which all the correlations have been well studied, correlations between different cryptocoins haven't been well understood yet. Moreover, correlations may change in different time period. As a result, to meet this desire, I decided to build this platform.

#### Use cases
1. A Tool for data scientists to acquire cryptocoins historical data.
2. An information provider system for real-time portfolio management system.
3. A web API, which returns a list of json objects with all the information, including price, volume, and correlations.



## Web service
### Interface
1. Show the *Correlation Matrix* as heatmap for 10 cryptocoins which have the top 10 market value.
2. Let user to look up *Correlation* (the latest one hour) by specifying *ID* of two coins.
3. Visualization of *Price* and *Volume* information vs *Time Interval* on a chart.
4. Let user to change the *Price* and *Volume* by specifying *ID* of a coin.

![heatmap](picture/heatmap.png)

![price_chart](picture/price_chart.png)

![volume_chart](picture/volume_chart.png)

![cois_info](picture/coins_info.png)



## Architecture
### Technologies
* Kafka
* Spark Streaming
* Cassandra
* Flask

### ETL Data Pipeline
![ETL data pipeline](picture/InsightArchitecture.png)



## Scalability
This data pipeline can scale up to 100x
Specs:
* Scalability: 100x
* 1 record / 100ms
* 2.6 MB/sec
* 700 GB/day




## Dependency
### Python library
* send http requests
```
pip install requests
```

* Kafka in python
```
pip install kafka-python
```

* Cassandra dirver in python
```
pip install cassandra-driver
```
