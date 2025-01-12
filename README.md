1.	This project will help us to understand the usage of traditional RDD APIs.
2.	We have a data file: sample.csv with no header and lib/log4j.properties 
3.	Creating a new spark conf object inside main function
4.	Dataframe APIs are based on SparkSession and RDD APIs are based on SparkContext.
5.	As SparkSession is a higher-level object which was created in newer versions of Spark as an improvement over SparkContext. So we can create a SparkSession and create SparkContext using that SparkSession object.
6.	RDD API leaves all responsibility to developers to structure the data, implement operations, create an optimized DS and compress the objects etc.
7.	We need to handcode everything, including regular operations like grouping and aggregating. 
8.	Spark Engine had no clue about DS inside RDD neither Spark could look inside your Lambda functions. These 2 things limited Spark to create an optimized execution plan.
