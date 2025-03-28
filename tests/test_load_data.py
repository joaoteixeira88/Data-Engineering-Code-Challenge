import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.crosscutting.constants import FieldConstants
from src.load_data import LoadData


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()


class TestLoadData:
    def setup_method(self):
        self.load_data = LoadData()

    def test_normalize_transaction_dates_valid_dates_return_all_normalized_date(
        self, spark
    ):
        # Arrange
        input_data = [
            ("1", "2024-07-04"),
            ("2", "2024/11/04"),
            ("3", "February 25, 2024"),
            ("4", "04-07-2024"),
            ("5", "11/04/2024"),
        ]
        input_df = spark.createDataFrame(
            input_data, [FieldConstants.TRANSACTION_ID, FieldConstants.TRANSACTION_DATE]
        )

        expected_data = [
            ("1", "2024-07-04"),
            ("2", "2024-11-04"),
            ("3", "2024-02-25"),
            ("4", "2024-07-04"),
            ("5", "2024-11-04"),
        ]
        expected_df = spark.createDataFrame(
            expected_data,
            [FieldConstants.TRANSACTION_ID, FieldConstants.TRANSACTION_DATE],
        )

        # Act
        result_df = self.load_data._normalize_transaction_dates(input_df)

        # Assert
        assert_df_equality(
            result_df.withColumn(
                FieldConstants.TRANSACTION_DATE,
                col(FieldConstants.TRANSACTION_DATE).cast("string"),
            ),
            expected_df,
            ignore_row_order=True,
            ignore_column_order=True,
        )

    def test_normalize_transaction_dates_invalid_dates_return_none(self, spark):
        # Arrange
        input_data = [("1", "2024-07-04"), ("2", "test")]
        input_df = spark.createDataFrame(
            input_data, [FieldConstants.TRANSACTION_ID, FieldConstants.TRANSACTION_DATE]
        )

        expected_data = [("1", "2024-07-04"), ("2", None)]
        expected_df = spark.createDataFrame(
            expected_data,
            [FieldConstants.TRANSACTION_ID, FieldConstants.TRANSACTION_DATE],
        )

        # Act
        result_df = self.load_data._normalize_transaction_dates(input_df)

        # Assert
        assert_df_equality(
            result_df.withColumn(
                FieldConstants.TRANSACTION_DATE,
                col(FieldConstants.TRANSACTION_DATE).cast("string"),
            ),
            expected_df,
            ignore_row_order=True,
            ignore_column_order=True,
        )
