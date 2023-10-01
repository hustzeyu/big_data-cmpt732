import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import broadcast, lit
import math

def sep_line(line):
    sd = line.split(":")
    result = []
    if len(sd) == 2:
        s = sd[0]
        dd = sd[1].split(" ")
        for d in dd:
            if ((d is not None) and (d != '')) :
                result.append((s, d))
    return result

def main(inputs, output, source, destination):
    edge_schema = types.StructType([
        types.StructField('source', types.StringType()),
        types.StructField('destination', types.StringType()),
    ])

    graph = sc.textFile(inputs)
    graph_df = spark.read.csv(inputs, sep=":", schema=edge_schema)
    edges_rdd = graph.flatMap(sep_line)
    e = edges_rdd.toDF(edge_schema).cache()
    e.createOrReplaceTempView("edge")

    ck_row = Row("node", "source", "dist")
    init_row = ck_row(source, source, 0)

    table_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('source', types.StringType()),
        types.StructField('dist', types.StringType()),
    ])

    temp = spark.createDataFrame([[source, None, 0]],table_schema )
    temp.createOrReplaceTempView("tmp")   
    checked = spark.createDataFrame([init_row],table_schema )
    checked.createOrReplaceTempView("ck")  


    N = graph_df.count()

    for i in range(N):
        temp = spark.sql("SELECT edge.destination AS node, tmp.node AS source, (tmp.dist + 1) AS dist "
                         "FROM tmp INNER JOIN edge "
                         "ON tmp.node = edge.source ").cache()
        temp.createOrReplaceTempView("tmp")
        multi = spark.sql("SELECT tmp.node, tmp.source, tmp.dist "
                         "FROM tmp INNER JOIN ck "
                         "ON tmp.node = ck.node ")
        multi.createOrReplaceTempView("ml")
        if(multi.count() > 0):
            uni = temp.substract(multi)
        else:
            uni = temp.select("*")
        checked = checked.unionAll(uni).cache()
        checked_rdd = checked.rdd
        checked_rdd.saveAsTextFile(output + '/iter-' + str(i))
        if (checked.where(checked["node"] == destination).count() > 0):
            break

    checked_rdd.take(10)

    paths = checked_rdd

    result = []
    result.append(destination)   
    dest = paths.lookup(destination)
    for j in range(N):
        pre = dest[0]
        print("pre = ", pre)
        print("source = ", source)
        result.append(pre)
        if (pre == source):
            break
        dest = paths.lookup(pre)

    print("!!!!! result = ", result)
    finalpath = sc.parallelize(reversed(result))
    finalpath.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, destination)