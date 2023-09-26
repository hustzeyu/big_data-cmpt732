from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_separate(line):
    ws = line.split()
    yield (ws[0], ws[1], ws[2], ws[3], ws[4])

def map_int(w):
    return (w[0], w[1], w[2], int(w[3]), w[4])

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t(%s, %s)" % (kv[0], kv[1][0], kv[1][1])

def get_max(l1, l2):
    return l1 if l1[0] > l2[0] else l2

text = sc.textFile(inputs)
words = text.flatMap(words_separate)
words_int = words.map(map_int)
words_filtered = words_int.filter(lambda v: v[1].lower()=="en" and v[2] != "Main_Page" and v[2].startswith("Special:")==False)
out_pairs = words_filtered.map(lambda x: (x[0], (x[3],x[2])))

print(out_pairs.take(10))

most_views = out_pairs.reduceByKey(get_max)
max_count = most_views.sortBy(get_key)
max_count.map(tab_separated).saveAsTextFile(output)
