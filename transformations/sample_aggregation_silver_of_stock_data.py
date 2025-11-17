from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from utilities import utils
from pyspark.sql.functions import split


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table
def sample_aggregation_silver_of_stock_data():
    df = spark.read.table("eunl_history")
    df = df.withColumnRenamed("Datum;Schluss", "Datum_Schluss")

    
    return (
        df.withColumn("Datum", split(col("Datum_Schluss"), ";").getItem(0))
        .withColumn("Schluss", split(col("Datum_Schluss"), ";").getItem(1))
        .drop("Datum_Schluss")
    )
