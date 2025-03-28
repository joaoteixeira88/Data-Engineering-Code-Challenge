import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src.crosscutting.constants import FieldConstants
from src.transformations import TransformationData


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()


@pytest.fixture
def sample_data(spark):
    """
    Fixture to create sample sales, products, and stores DataFrames for testing.
    """

    sales_data = [
        ("t1", "s1", "p1", 2, "2024-07-04", 50.0),
        ("t2", "s2", "p2", 3, "2024-07-04", 10.0),
        ("t3", "s1", "p1", 1, "2024-07-04", 50.0),
    ]
    sales_df = spark.createDataFrame(
        sales_data,
        [
            FieldConstants.TRANSACTION_ID,
            FieldConstants.STORE_ID,
            "product_id",
            FieldConstants.QUANTITY,
            FieldConstants.TRANSACTION_DATE,
            FieldConstants.PRICE,
        ],
    )

    product_data = [
        ("p1", "Computer", "Electronics"),
        ("p2", "Shirt", "Clothing"),
    ]
    products_df = spark.createDataFrame(
        product_data,
        ["product_id", FieldConstants.PRODUCT_NAME, FieldConstants.CATEGORY],
    )

    store_data = [
        ("s1", "Store A", "Lisbon"),
        ("s2", "Store B", "Porto"),
    ]
    stores_df = spark.createDataFrame(
        store_data, [FieldConstants.STORE_ID, "store_name", FieldConstants.LOCATION]
    )

    return sales_df, products_df, stores_df


@pytest.fixture
def transformation_data(sample_data):
    """
    Fixture to initialize the TransformationData class with sample data.
    """
    sales_df, products_df, stores_df = sample_data
    return TransformationData(sales_df, products_df, stores_df)


def test_calculate_total_revenue(transformation_data, spark):
    # Arrange
    expected_data = [
        ("s1", "Electronics", 150.0),
        ("s2", "Clothing", 30.0),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            FieldConstants.STORE_ID,
            FieldConstants.CATEGORY,
            FieldConstants.TOTAL_REVENUE,
        ],
    )

    # Act
    result_df = transformation_data.calculate_total_revenue()

    # Assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_calculate_monthly_sales(transformation_data, spark):
    # Arrange
    expected_data = [
        (2024, 7, "Electronics", 3),
        (2024, 7, "Clothing", 3),
    ]
    field = [
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("total_quantity_sold", LongType(), True),
    ]
    schema = StructType(field)

    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Act
    result_df = transformation_data.calculate_monthly_sales()

    # Assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_enrich_sales_data(transformation_data, spark):
    # Arrange
    expected_data = [
        ("t1", "Store A", "Lisbon", "Computer", "Electronics", 2, "2024-07-04", 50.0),
        ("t2", "Store B", "Porto", "Shirt", "Clothing", 3, "2024-07-04", 10.0),
        ("t3", "Store A", "Lisbon", "Computer", "Electronics", 1, "2024-07-04", 50.0),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            FieldConstants.TRANSACTION_ID,
            "store_name",
            FieldConstants.LOCATION,
            FieldConstants.PRODUCT_NAME,
            FieldConstants.CATEGORY,
            FieldConstants.QUANTITY,
            FieldConstants.TRANSACTION_DATE,
            FieldConstants.PRICE,
        ],
    )

    # Act
    result_df = transformation_data.enrich_sales_data()

    # Assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_add_price_category(transformation_data, spark):
    # Arrange
    input_data = [
        ("t1", 10.0),
        ("t2", 50.0),
        ("t3", 200.0),
    ]
    input_df = spark.createDataFrame(
        input_data, [FieldConstants.TRANSACTION_ID, FieldConstants.PRICE]
    )

    expected_data = [
        ("t1", 10.0, "Low"),
        ("t2", 50.0, "Medium"),
        ("t3", 200.0, "High"),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            FieldConstants.TRANSACTION_ID,
            FieldConstants.PRICE,
            FieldConstants.PRICE_CATEGORY,
        ],
    )

    # Act
    result_df = transformation_data.add_price_category(input_df)

    # Assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
