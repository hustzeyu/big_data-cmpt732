import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)  
    weather = weather.filter(weather["qflag"].isNull()).cache()
    weather_s = weather.select(weather["date"],weather["station"],weather["value"],weather["observation"])
    tb_max = weather_s.filter(weather_s["observation"]=="TMAX").withColumnRenamed("value", "max_value")
    tb_min = weather_s.filter(weather_s["observation"]=="TMIN").withColumnRenamed("value", "min_value")
    tb_diff = tb_max.join(tb_min, ["date", "station"], "inner").cache()
    tb_s = tb_diff.select(tb_diff["date"], tb_diff["station"], ((tb_diff["max_value"]-tb_diff["min_value"])/10).alias("range")).cache()
    range_max = tb_s.groupBy(tb_s["date"]).agg(functions.max(tb_s["range"]).alias("range"))
    tb_join = tb_s.join(range_max, ["date", "range"], "inner")
    tb_final = tb_join.sort(tb_join["date"]).select(tb_join["date"], tb_join["station"], tb_join["range"])
    tb_final.show(10)
    tb_final.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)