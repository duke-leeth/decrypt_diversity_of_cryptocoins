# insight_project


## Week 1 Deliverable
### Project Idea (1-sentence)
To provide a real-time cryptocoins analysis platform for data scientists to retrieve data, and offer a tools for portfolio diversification and risk reduction.


### What is the purpose, and most common use cases?
#### Purpose
Whenever investors want to reduce the risk by diversifying portfolio, they need to know the correlation between all the investment targets. However, unlike the stock market, in which all the correlations have been well studied, correlations between different cryptocoins haven't been well understood yet. Moreover, the correlations may change in different time period. To meet this desire, I decide to build the platform.

#### Use cases
1. A Tool for data scientists to acquire cryptocoin historical data.
2. An information provider system for real-time portfolio management system.
3. *(If applicable)* API which return a list of json objects with all the information stated above, and can allow 1K users to access simultaneously.


### Problem Statement
Given *Time Interval*, *Sampling Rate (Resolution)* and *A set of coins*, return a table of *Price*, *Volumn*, and *Correlation Matrix*.

#### Interface
1. Let user to specify *Time Interval*, *Sampling Rate (Resolution)* and *A set of coins*
2. Visulization of *Price* and *Volumn* information vs *Time Interval* on a diagram
3. Animate heatmap of *Correlation Matrix* during *Time Interval*.
4. **Default Parameter** Display animation of the top 10 highest correlated coins within 24 hrs, with 1 min of resolution.


### Which technologies are well-suited to solve those challenges? (list all relevant)
* Kafka
* Cassandra
* Flask
* Node.js
* Streaming Analytics


### Proposed Architecture
![Proposed Architecture](picture/InsightArchitecture.png)


### Dependency

send http requests
```
pip install requests
```

Kafka in python
```
pip install kafka-python
```

Cassandra dirver in python
```
pip install cassandra-driver
```
