from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pulse_etl import main

if __name__ == "__main__":
    conf = SparkConf().setAppName('pulse_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    tpt = main.etl_job(sc, sqlContext)

