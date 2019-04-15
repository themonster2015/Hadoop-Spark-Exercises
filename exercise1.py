
# -*- coding: utf-8 -*-
from __future__ import print_function,division
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as F
import sys
import pyspark


def main():
    if len(sys.argv) != 5:
    	print("How to use: spark-submit --master local[*] --num-executors 4 --driver-memory 4g python/exercise1.py patentes-mini/cite75_99.txt patentes-mini/apat63_99.txt dfCitas.parquet dfInfo.parquet", file=sys.stderr)
    	exit(-1)
   	print ("This is the name of the script: ",sys.argv[0])
    print ("Number of arguments: ",len(sys.argv))
    print ("The arguments are: ", str(sys.argv))
    rutapatent = sys.argv[1]
    rutaapat = sys.argv[2]
    rutacitaparquet = sys.argv[3]
    rutainfoparquet = sys.argv[4]
    spark = SparkSession\
    .builder\
    .appName("Exercise 1")\
    .getOrCreate()
    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")
    patents = spark.read.option('inferSchema', 'true').option('header',
        'true').csv(rutapatent)
    df_grouped=patents.select('CITING','CITED').groupBy('CITED').count()
    df_grouped.show()
    df_grouped.count()
    df = df_grouped.sort("count", ascending=False).collect()
    df = df_grouped.select(expr("CITED AS NPatente"),expr('count AS ncitas'))
    df.show(5)
    df2 = df.repartition(2)
    df.write.parquet(rutacitaparquet, mode='overwrite')
    apat = spark.read.option('inferSchema', 'true').option('header',
		'true').csv(rutaapat)
    apat.printSchema()
    apat.show(5)
    apat.count()
    selected = apat.select(
		expr("PATENT AS NPatente"),
		expr('COUNTRY AS Pais'),
		expr('APPYEAR AS Ahno' ))
    selected.show(5)
    selected2 = selected.repartition(2)
    selected2.write.format("parquet")\
	.mode("overwrite")\
	.option("compression", "gzip")\
	.save(rutainfoparquet)

	#df2.write.mode('overwrite').format('parquet').option('compression','gzip').save(rutacitaparquet)
	# #print second data frame

	

if __name__ == "__main__":
	main()
