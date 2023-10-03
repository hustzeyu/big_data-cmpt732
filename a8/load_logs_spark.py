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

def split_line(line):
    lines = line_re.split(line)
    if len(lines) > 4:
        HostName = lines[1]
        dt = datetime.datetime.strptime(lines[2], '%d/%b/%Y:%H:%M:%S')
        path = lines[3]
        bytes = int(lines[4])
        Uuid = str(uuid.uuid4())
        return (HostName, dt, path, bytes, Uuid)

def main(input1, input2, input3):
    keyspace = input2
    tablename = input3
    cluster = Cluster(['node1.local', 'node2.local'])   
    session = cluster.connect(keyspace)
    rdd = sc.textFile(input1)
    rdd_repartition = rdd.repartition(64)
    rdd_split = rdd_repartition.map(split_line).filter(lambda x: x is not None)
    col_names = ["host", "datetime", "path", "bytes", "uuid"]
    logs_df = rdd_split.toDF(col_names)
    logs_df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table=tablename, keyspace=keyspace).save()

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    input3 = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+ls
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1, input2, input3)