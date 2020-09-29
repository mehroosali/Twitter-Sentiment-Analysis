# Twitter-Sentiment-Analysis
 In this project, using Nifi as a data ingestion tool, I have pulled live twitter tweets from Twitter API and pushed to Kafka topics which is then consumed by a Spark Streaming application. Basic sentiment analysis is performed on the data using Stanford NLP library and final result are stored in HDFS and MySQL. Also the results are pushed to Elasticsearch to perform visualization using Kibana.
![alt text](https://iili.io/21YmrB.png)
