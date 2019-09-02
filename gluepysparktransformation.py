import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "purchasedata", table_name = "raw_purchase_data", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "purchasedata", table_name = "raw_purchase_data", transformation_ctx = "datasource0")


transf = Filter.apply(frame = datasource0, f = lambda x: x["age"] >= 18 , transformation_ctx = "transf")



## @type: ApplyMapping
## @args: [mapping = [("age", "long", "age", "long"), ("salary", "long", "salary", "long"), ("purchased", "long", "purchased", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = transf, mappings = [("age", "long", "age", "long"), ("salary", "long", "salary", "long"), ("purchased", "long", "purchased", "long")], transformation_ctx = "applymapping1")


## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://transformed-purchase-data"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://transformed-purchase-data"}, format = "json", transformation_ctx = "datasink2")
job.commit()