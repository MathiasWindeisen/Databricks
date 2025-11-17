from pyspark import pipelines as dp
from pyspark.sql.functions import col

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table
def sample_users_silver_of_stock_data():
    return (
        spark.read.table("samples.wanderbricks.users")
        .select("user_id", "email", "name", "user_type")
    )
