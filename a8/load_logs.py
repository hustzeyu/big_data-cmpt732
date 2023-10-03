import sys
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#from pyspark.sql import SparkSession, functions, types
#from pyspark.sql.functions import broadcast, lit
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
        return [HostName, dt, path, bytes, Uuid]


def main(input1, input2, input3):
    keyspace = input2
    tablename = input3
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    insert = session.prepare("INSERT INTO " + tablename + "(host, datetime, path, bytes, uuid) VALUES (?, ?, ?, ?, ?)")
    cnt = 0
    batchaction = BatchStatement()

    for f in os.listdir(input1):
        with gzip.open(os.path.join(input1, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                lines = split_line(line)
                if lines is not None:
                    cnt += 1
                    batchaction.add(insert, (lines[0],lines[1],lines[2],lines[3],lines[4]))
                if cnt >= batchsize:
                    cnt = 0
                    session.execute(batchaction)
                    batchaction = BatchStatement()
                    
    if cnt > 0:
        session.execute(batchaction)
        batchaction = BatchStatement()

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    input3 = sys.argv[3]
 #   spark = SparkSession.builder.appName('load logs').getOrCreate()
 #   assert spark.version >= '3.0' # make sure we have Spark 3.0+ls
 #   spark.sparkContext.setLogLevel('WARN')
 #   sc = spark.sparkContext
    main(input1, input2, input3)