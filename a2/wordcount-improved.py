from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

conf = SparkConf().setAppName('Word Count Improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once_improved(line):
    ws = wordsep.split(line)
    for w in ws:
        yield (w.lower(), 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once_improved)
words_new = words.filter(lambda x: x!="")
wordcountimproved = words_new.reduceByKey(add)

outdata = wordcountimproved.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)