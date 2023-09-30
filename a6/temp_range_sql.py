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
    weather.createOrReplaceTempView("TB_weather")

    weather_qflag_null = spark.sql("SELECT date, station, observation, value"
                        "FROM TB_weather"
                        "WHERE qflag IS NULL").cache()
    weather_qflag_null.createOrReplaceTempView("TB_qflag_null")

    tb_max = spark.sql("SELECT date, station, observation, value AS max_value"
                        "FROM TB_qflag_null"
                        "WHERE observation = 'TMAX'")
    tb_max.createOrReplaceTempView("TB_max")

    tb_min = spark.sql("SELECT date, station, observation, value AS min_value"
                        "FROM TB_qflag_null"
                        "WHERE observation = 'TMIN'")
    tb_min.createOrReplaceTempView("TB_min")

    tb_diff = spark.sql("SELECT TB_max.date AS date, TB_max.station AS station, (TB_max.max_value - TB_min.min_value) / 10 AS range"
                        "FROM TB_max INNER JOIN TB_min"
                        "ON TB_max.date = min.date AND TB_max.station = min.station").cache()
    tb_diff.createOrReplaceTempView("TB_diff")

    tb_range = spark.sql("SELECT date, max(range) AS range"
                        "FROM TB_diff"
                        "GROUP BY date")
    tb_range.createOrReplaceTempView("TB_RANGE")

    tb_final = spark.sql("SELECT TB_diff.date, TB_diff.station, TB_RANGE.range"
                        "FROM TB_diff INNER JOIN TB_RANGE"
                        "ON TB_diff.date = TB_RANGE.date AND TB_diff.range = TB_RANGE.range"
                        "ORDER BY TB_diff.date")
                        
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