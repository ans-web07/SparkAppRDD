import os
import sys
from collections import namedtuple

from pyspark import *
from pyspark.sql import *

from lib.logger import Log4j

#schema for RDD
SurveyRecord = namedtuple("SurveyRecord",["Age","Gender","Country","State"])

if __name__ == "__main__":

    # Creating a new spark conf object
    conf1 = SparkConf()
    conf1.setMaster("local[3]") \
        .setAppName("HelloRDD")

    ''' When we use Dataframe API-->based on SparkSession
        spark= SparkSession.builder.config(conf=conf).getOrCreate()
        When we use RDD API-->based on SparkContext
        sc= SparkContext(conf=conf1)
    '''

    # As SparkSession is a higher-level object which was created
    # in newer versions of Spark as an improvement over SparkContext.
    # So we can create a SparkSession and create  SparkContext using that SparkSession object.

    spark = SparkSession.builder \
        .config(conf=conf1) \
        .getOrCreate()

    sc = spark.sparkContext

    logger = Log4j(spark)

    LOG_LEVEL = "INFO"
    spark.sparkContext.setLogLevel(LOG_LEVEL)

    # if for checking cmd line arg, if cmd line arg is not provided err is logged
    # Validate file existence
    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        logger.error(f"Input file '{input_file}' does not exist or is not accessible.")
        sys.exit(-1)

    #SparkContext comes with bunch of methods to read binary,sequence,hadoop,object file
    #RDD creation
    linesRDD = sc.textFile(sys.argv[1])

    #RDD processing
    '''
    Most of RDD trans were designed to accept Lambda func and apply custom code to RDD
    RDD offers only basic transformations such as map(),reduce(),filter(),foreach() etc.
    RDD API leaves all responsibility in developer's hand to give a structure to data, implement ops,
    create an optimized DS and compress your objects etc.
    '''
    part_RDD = linesRDD.repartition(2)

    #giving structure to data record: Removing double quotes and splitting the line
    #i/p= line of text o/p= list of text
    colsRDD=  part_RDD.map(lambda line: line.replace('"','').split(","))

    #Attaching schema to RDD and implementing select operation as we have only selected 4 cols
    selectRDD =colsRDD.map(lambda cols:SurveyRecord(int(cols[1],cols[2],cols[3],cols[4])))

    #Filtering records
    filterRDD = selectRDD.filter(lambda r: r.Age < 40)

    #Grouping the record by country and count it

    #Country is key and value is hardcoded value 1
    keyvalueRDD = filterRDD.map(lambda r:(r.Country,1))
    #key = country , value= sum of all values of country
    countRDD = keyvalueRDD.reduceByKey(lambda v1,v2: v1+v2)

    #Collecting results and pushing it to log file
    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)







