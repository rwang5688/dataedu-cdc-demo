"""

author@Kyle E.


This code takes a path from your Data Lake and converts it to 'delta' format which is a pre-requisite for this.

Feel free to edit the file formats. For this demo, we will use a standard csv file

"""

from delta import *
from pyspark.sql.session import SparkSession


spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Read Source
inputDF = spark.read.format("csv").option("header", "true").load('s3://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/data/raw/')

# Write data as a DELTA TABLE
inputDF.write.format("delta").mode("overwrite").save("s3a://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/delta/current/")

# Read Source
updatesDF = spark.read.format("csv").option("header", "true").load('s3://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/data/updates/')

# Write data as a DELTA TABLE
updatesDF.write.format("delta").mode("overwrite").save("s3a://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/delta/updates_delta/")

# Generate MANIFEST file for Athena/Catalog
deltaTable = DeltaTable.forPath(spark, "s3a://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/delta/current/")
deltaTable.generate("symlink_format_manifest")

### OPTIONAL, UNCOMMENT IF YOU WANT TO VIEW ALSO THE DATA FOR UPDATES IN ATHENA
###
# Generate MANIFEST file for Updates
updatesDeltaTable = DeltaTable.forPath(spark, "s3a://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/delta/updates_delta/")
updatesDeltaTable.generate("symlink_format_manifest")
