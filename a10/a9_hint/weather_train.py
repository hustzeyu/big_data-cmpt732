import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import *


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, model_file):
    # get the data
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    yesterday_query = 'SELECT today.station , dayofyear(today.date) AS dayofyear, today.latitude AS latitude, today.longitude AS longitude, today.elevation AS elevation, today.tmax AS tmax, \
                        yesterday.tmax AS yesterday_tmax \
                        FROM __THIS__ as today \
                        INNER JOIN __THIS__ as yesterday \
                        ON date_sub(today.date, 1) = yesterday.date \
                        AND today.station = yesterday.station'

#    yesterday_query = 'SELECT today.station , dayofyear(today.date) AS dayofyear, today.latitude AS latitude, today.longitude AS longitude, today.elevation AS elevation, today.tmax AS tmax \
#                        FROM __THIS__ as today;'

    sqlTrans = SQLTransformer(statement=yesterday_query)

#    w_assembler = VectorAssembler(inputCols=['dayofyear', 'latitude', 'longitude','elevation'], outputCol='features')
    w_assembler = VectorAssembler(inputCols=['dayofyear', 'latitude', 'longitude','elevation','yesterday_tmax'], outputCol='features')
    Regressor = DecisionTreeRegressor(featuresCol='features', labelCol='tmax',  maxDepth=10)
    w_pipeline = Pipeline(stages=[sqlTrans, w_assembler, Regressor])
    model = w_pipeline.fit(train)

    predictions = model.transform(validation)

    predictions.show()
    
    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]

#    inputs = '/Users/zeyuhu/Documents/sfu1/732/a10/a9_hint/tmax-1'
#    model_file = 'weather-model'
    main(inputs, model_file)