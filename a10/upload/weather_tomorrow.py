import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import *
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta


today = date.today()
yesterday = today - timedelta(days=1)

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data

    test_data = [("CA001101158", yesterday, 49.2771, -122.9146, 330.0, 6.0), ("CA001101158", today, 49.2771, -122.9146, 330.0, 7.0)]

    test_tmax = spark.createDataFrame(data=test_data, schema=tmax_schema)
    test_tmax.cache()


    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(test_tmax)
    prediction = predictions.select("prediction").collect()
    print('Predicted tmax tomorrow:', prediction[0][0])
    



if __name__ == '__main__':
    model_file = sys.argv[1]
    
    test_model(model_file)