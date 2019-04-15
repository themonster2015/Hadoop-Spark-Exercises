
# -*- coding: utf-8 -*-
from __future__ import print_function,division
from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
# from pyspark.sql.functions import col
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys
import pyspark


def main():
    if len(sys.argv) != 3:
    	print("How to use: spark-submit --master local[*] --num-executors 4 --driver-memory 4g python/exercise3.py patentes-mini/apat63_99.txt python/saved", file=sys.stderr)
    	exit(-1)
   	print ("This is the name of the script: ",sys.argv[0])
    print ("Number of arguments: ",len(sys.argv))
    print ("The arguments are: ", str(sys.argv))
    dirApat = sys.argv[1]
    dirSave = sys.argv[2]
    spark = SparkSession\
    .builder\
    .appName("Exercise 3")\
    .getOrCreate()
    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL") 
    rdd = spark.read.csv(dirApat, header=True).coalesce(8).rdd
    prdd = rdd.map(lambda x: (x[4],x[1]))
    prdd = prdd.map(lambda x: ((x[0], x[1]), 1))
    prdd=prdd.reduceByKey(lambda x, y: x + y).sortByKey()
    prdd = prdd.map(lambda ((x,y),z): (x,(y,z)))
    prdd = prdd.groupByKey().sortByKey()
    print
    lista = [(k, list(v)) for k, v in prdd.collect()]
    print(lista)
    prdd.saveAsTextFile(dirSave)


if __name__ == "__main__":
	main()
