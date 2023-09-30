import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    date_hour = path.split("/")[-1].strip("pagecounts-").strip("")
    date_hour = date_hour[:-4]
    return date_hour

def main(inputs, output):
    comments_schema = types.StructType([
        types.StructField("language", types.StringType()),
        types.StructField("Main_Page", types.StringType()),
        types.StructField("views", types.LongType()),
        types.StructField("size", types.DoubleType()),  
    ])

    wiki = spark.read.csv(inputs, schema=comments_schema, sep = " ").withColumn("filename", functions.input_file_name())


    wiki = wiki.where(wiki.language =="en")
    wiki = wiki.where(wiki.Main_Page.startswith("Special:")==False)
    wiki = wiki.where(wiki.Main_Page !="Main_Page")
    wiki = wiki.withColumn("date_hour",path_to_hour(wiki["filename"])).withColumnRenamed("Main_Page","title").cache()
    wiki_max = wiki.groupBy("date_hour").agg(functions.max("views")).withColumnRenamed("max(views)","max_views").withColumnRenamed("date_hour","hour")
    wiki_out = wiki.join(broadcast(wiki_max), (wiki.date_hour==wiki_max.hour) & (wiki.views==wiki_max.max_views),"inner")
    #wiki_out = wiki.join(wiki_max, (wiki.date_hour==wiki_max.hour) & (wiki.views==wiki_max.max_views),"inner")
    wiki_out = wiki_out.select(wiki_out.hour, wiki_out.title, wiki_out.views)
    wiki_out.explain()
    #wiki_out.write.json(output, mode='overwrite')
    wiki_out.coalesce(1).write.format('json').save(output)



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit averages').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+ls
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)