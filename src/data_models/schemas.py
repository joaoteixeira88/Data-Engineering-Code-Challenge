from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("transaction_date", StringType(), False),
    StructField("price", DoubleType(), False)

])

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False)
])

store_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("location", StringType(), False)
])