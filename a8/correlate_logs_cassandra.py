import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast, lit
import os
import gzip, uuid
import math
import re
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
from cassandra.cluster import Cluster
import datetime
from cassandra.query import BatchStatement
batchsize = 200

def cal_r(six_sums):
    n = six_sums.head()[0]
    x_sum = six_sums.head()[1]
    x2_sum = six_sums.head()[2]
    y_sum = six_sums.head()[3]
    y2_sum = six_sums.head()[4]
    xy_sum = six_sums.head()[5]
    r = ((n * xy_sum)-(x_sum * y_sum))/((math.sqrt(n * x2_sum - x_sum * x_sum))*(math.sqrt(n * y2_sum - y_sum * y_sum)))
    r2  = r*r
    return r, r2 


def main(input1, input2):
    keyspace = input1
    tablename = input2
    cluster = Cluster(['node1.local', 'node2.local'])   
    session = cluster.connect(keyspace)
    logs_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=keyspace).load()
    logs_df_sum = logs_df.groupBy("host").agg(functions.count("host").alias("cnt"),functions.sum("bytes").alias("sum_bytes"))
    six_values = logs_df_sum.select(lit(1).alias("n"),logs_df_sum["cnt"].alias("x"), (logs_df_sum["cnt"]*logs_df_sum["cnt"]).alias("x2"), logs_df_sum["sum_bytes"].alias("y"), (logs_df_sum["sum_bytes"]*logs_df_sum["sum_bytes"]).alias("y2"), (logs_df_sum["cnt"]*logs_df_sum["sum_bytes"]).alias("xy"))
    six_sums = six_values.groupBy().sum()
    r, r2 = cal_r(six_sums)
    print("r =", r)
    print("r2 =", r2)


if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]

    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+ls
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1, input2)