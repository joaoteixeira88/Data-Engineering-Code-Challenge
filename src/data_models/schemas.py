from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.crosscutting.constants import FieldConstants

transaction_schema = StructType(
    [
        StructField(FieldConstants.TRANSACTION_ID, StringType(), False),
        StructField(FieldConstants.STORE_ID, StringType(), False),
        StructField(FieldConstants.PRODUCT_ID, StringType(), False),
        StructField(FieldConstants.QUANTITY, IntegerType(), False),
        StructField(FieldConstants.TRANSACTION_DATE, StringType(), False),
        StructField(FieldConstants.PRICE, DoubleType(), False),
    ]
)

product_schema = StructType(
    [
        StructField(FieldConstants.PRODUCT_ID, StringType(), False),
        StructField(FieldConstants.PRODUCT_NAME, StringType(), False),
        StructField(FieldConstants.CATEGORY, StringType(), False),
    ]
)

store_schema = StructType(
    [
        StructField(FieldConstants.STORE_ID, StringType(), False),
        StructField(FieldConstants.STORE_NAME, StringType(), False),
        StructField(FieldConstants.LOCATION, StringType(), False),
    ]
)
