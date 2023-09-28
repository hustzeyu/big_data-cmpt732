from pyspark import SparkConf, SparkContext
import json
import re, string
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def reddits_split(lines):
    sp = json.loads(lines)
    k_subreddit = sp["subreddit"]
    k_score = float(sp["score"])
    k_author = sp["author"]
    yield (k_subreddit,k_score,k_author)

def main(inputs, output):
    # main logic starts here

    text = sc.textFile(inputs)
    sp_text = text.flatMap(reddits_split).filter(lambda x: "e" in x[0]).cache()
 #   sp_text = text.flatMap(reddits_split).filter(lambda x: "e" in x[0])
    sp1 = sp_text.filter(lambda x: x[1] > 0)
    sp2 = sp_text.filter(lambda x: x[1] <= 0)

    sp1.map(json.dumps).saveAsTextFile(output + '/positive')
    sp2.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)