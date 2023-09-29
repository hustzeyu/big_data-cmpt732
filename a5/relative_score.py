from pyspark import SparkConf, SparkContext
import json
import re, string
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def create_pairs(j):
    subreddit = j["subreddit"]
    score = j["score"]
    return (subreddit, (1, score))

def add_pairs(a, b):
    cnt = a[0] + b[0]
    sum = a[1] + b[1]
    return (cnt, sum)

def positive_ave(kv):
    if kv[1] > 0:
        return True
    else:
        return False


def get_average(kv):
    key = kv[0]
    pair = kv[1]
    cnt = pair[0]
    sum = pair[1]
    return (key, sum/cnt)

def rltv_scr(w):
    score = w[1][0]["score"]
    average = w[1][1]
    author = w[1][0]["author"]
    relative_score = score / average
    return (relative_score, author)


def get_key(kv):
    return kv[0]

def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata = text.map(json.loads).cache()
    pairs = commentdata.map(create_pairs)
    reduce_pair = pairs.reduceByKey(add_pairs)

    positive_average = reduce_pair.map(get_average).filter(lambda x: x[1] > 0)

    commentbysub = commentdata.map(lambda c: (c['subreddit'], c)).join(positive_average)
    outdata = commentbysub.map(rltv_scr).sortBy(get_key, ascending=False)

    outdata.map(json.dumps).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)