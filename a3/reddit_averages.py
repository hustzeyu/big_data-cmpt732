from pyspark import SparkConf, SparkContext
import json
import re, string
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def reddits_split(lines):
    sp = json.loads(lines)
    key = sp['subreddit']
    score = sp['score']
    yield (key, (1,score))

def add_pairs(a, b):
    return (a[0]+b[0], a[1]+b[1])

def cal_average(kv):
    average_score = float(kv[1][1]/kv[1][0])
    return (kv[0], average_score)



def get_key(kv):
    return kv[0]

def output_json(kv):
    return json.dumps(kv)

def main(inputs, output):
    # main logic starts here

    text = sc.textFile(inputs)
    sp_text = text.flatMap(reddits_split)
    average_reddit = sp_text.reduceByKey(add_pairs).map(cal_average)
    outdata = average_reddit.sortBy(get_key).map(output_json)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


