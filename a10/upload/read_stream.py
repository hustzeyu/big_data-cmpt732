import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast, lit
import os
import gzip, uuid
import math
import re
from kafka import KafkaConsumer

def cal(df_xy):

    df_init = df_xy.select(df_xy['x'], df_xy['y'], (df_xy['x']*df_xy['y']).alias('xy'), (df_xy['x']*df_xy['x']).alias('x2')) 

    df_sum =  df_init.select(functions.sum(df_init['x']).alias('sx'), functions.sum(df_init['y']).alias('sy'), functions.sum(df_init['xy']).alias('sxy'), functions.sum(df_init['x2']).alias('sx2'), functions.count(df_init['x2']).alias('n'))
    df_beta_sum = df_sum.select( ((df_sum['sxy']- (df_sum['sx']*df_sum['sy']/df_sum['n']) ) /(df_sum['sx2'] - (df_sum['sx']*df_sum['sx']/df_sum['n']))).alias('beta'), df_sum['*'])
    df_ab = df_beta_sum.select(df_beta_sum['beta'], ((df_beta_sum['sy']/df_beta_sum['n'])-(df_beta_sum['beta']*df_beta_sum['sx']/df_beta_sum['n'])).alias('alpha'))

    return df_ab

def main(input1):
    topic = input1

    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    df_split = functions.split(values['value'], ' ')
    df_xy = values.withColumn('x', df_split.getItem(0)).withColumn('y', df_split.getItem(1))
    df_ab = cal(df_xy)

    stream = df_ab.writeStream.outputMode("complete").format("console").start()
    stream.awaitTermination(600)
    #df_ab.show()

if __name__ == '__main__':
    input1 = sys.argv[1]
    spark = SparkSession.builder.appName('kafka').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+ls
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1)