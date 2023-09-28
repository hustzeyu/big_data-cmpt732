from pyspark import SparkConf, SparkContext
import sys
import re, string
import random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def add(partition):
    cnt = 0
    random.seed()
    for i in range(len(partition)):
        sum = 0
        while (sum < 1):
            sum += random.random()
            cnt += 1
    yield cnt


def main(samples):
    # main logic starts here
    scrdd = sc.parallelize(range(samples), 100)
    sep_sum = scrdd.mapPartitions(add)
    total_iterations = sep_sum.reduce(lambda x, y: x + y )
    print(total_iterations/samples)


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcountImproved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    n = int(sys.argv[1])
    main(n)