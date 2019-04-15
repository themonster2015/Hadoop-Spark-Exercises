
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
    if len(sys.argv) != 5:
    	print("How to use: spark-submit --master local[*] --num-executors 8 --driver-memory 4g --queue urgent python/exercise2.py python/dfCitas.parquet python/dfInfo.parquet country-codes.csv p2out", file=sys.stderr)
    	exit(-1)
   	print ("This is the name of the script: ",sys.argv[0])
    print ("Number of arguments: ",len(sys.argv))
    print ("The arguments are: ", str(sys.argv))
    dirCita = sys.argv[1]
    dirInfo = sys.argv[2]
    countrycode = sys.argv[3]
    out = sys.argv[4]
    spark = SparkSession\
    .builder\
    .appName("Show patents countries and numbers of citations")\
    .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL") 
    selected2 = spark.read\
                .format("parquet")\
                .option("mode", "FAILFAST")\
                .load(dirInfo)
    selected2.cache()
    a = (spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(countrycode))
    a = a.select(col("Code").alias("Pais"), col("Name").alias("NombrePais"))
    df2 = spark.read\
                .format("parquet")\
                .option("mode", "FAILFAST")\
                .load(dirCita)
    df2.cache()
    pais = col("Pais")
    citas = col("ncitas")
    ta = selected2.alias('tb')
    tb = a.alias('ta')
    join2 = selected2.join(a,'Pais' )
    df = join2.drop("Pais")
    df = df.withColumnRenamed('NombrePais', 'Pais')
    df  = df.join(df2,'NPatente' )
    #df.show()
    df = df.filter("NPatente is not NULL")
    df = df.filter("Pais is not NULL")
    df = df.filter("Ahno is not NULL")
    df.na.drop()
    df_agg = df.groupby('Pais','Ahno').\
        agg(
            F.count('NPatente').alias("NumPatentes")
        )
    #print(type(df_agg))
    #df_agg.printSchema()
    #df_agg.sort(asc("Pais")).show()
    df_agg2 = df.groupby( 'Pais', 'Ahno').\
        agg(
            F.sum('ncitas').alias("TotalCitas"),
            F.max('ncitas').alias("MaxCitas")
        )
    #df_agg2.sort(asc("Pais")).show()
    dff = df_agg2.join(df_agg,['Pais', 'Ahno'])
    dff= dff.withColumn("MediaCitas", dff.TotalCitas/dff.NumPatentes)
    dff.select("Pais","Ahno","NumPatentes","TotalCitas", "MediaCitas","MaxCitas").sort(desc("Pais")).show()
    dff.select("Pais","Ahno","NumPatentes","TotalCitas", "MediaCitas","MaxCitas").sort(asc("Pais")).show()
    dff.coalesce(1).write.format('com.databricks.spark.csv').save(out,header = 'true')


if __name__ == "__main__":
	main()
