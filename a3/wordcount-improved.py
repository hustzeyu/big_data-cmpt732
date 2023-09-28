from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))


# add more functions as necessary
def word_once(line):
    words = wordsep.split(line)
    for w in words:
        yield (w.lower(), 1)

def add(a, b):
    return a + b 

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return "%s %i" % (k, v)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs).repartition(10)
    sp_text = text.flatMap(word_once).filter(lambda x: x != "")
    wordcount = sp_text.reduceByKey(add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcountImproved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)