# Data Pipeline Based On Messaging Using PySpark And Hive Covid-19 Analysis 


The purpose is to collect the real time streaming data from COVID19 open API for every 5
minutes into the ecosystem using NiFi and to process it and store it in the data lake on
AWS.Data processing includes parsing the data from complex JSON format to csv format then
publishing to Kafka for persistent delivery of messages into PySpark for further processing.The
processed data is then fed into output Kafka topic which is inturn consumed by Nifi and stored in
HDFS .A Hive external table is created on top of HDFS processed data for which the process is
Orchestrated using Airflow to run for every time interval. Finally KPIs are visualised in tableau.
