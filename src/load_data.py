from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, when

from src.crosscutting.constants import FieldConstants
from src.crosscutting.logger import logger
from src.crosscutting.spark_singleton import spark
from src.data_models.schemas import product_schema, store_schema, transaction_schema


class LoadData:
    """
    A class responsible for loading and processing sales, product, and store data using PySpark.
    """

    def __init__(self):
        """
        Initializes a SparkSession and sets up placeholders for transactions, products, and stores data.
        """

        self.products = None
        self.transactions = None
        self.stores = None

    def load_data(self):
        """
        Loads sales transactions, products, and stores data from CSV files into Spark DataFrames.

        The method also applies transformations:
        - Normalizes transaction dates
        - Filters out transactions with non-positive quantity
        - Removes duplicate records
        """
        self.transactions = self._load_csv(
            path="data/sales_uuid.csv", schema=transaction_schema
        )
        self.transactions = self._normalize_transaction_dates(df=self.transactions)
        self.transactions = self.transactions.filter(
            self.transactions.quantity > 0
        ).dropDuplicates()

        self.products = self._load_csv(
            path="data/products_uuid.csv", schema=product_schema
        ).dropDuplicates()
        self.stores = self._load_csv(
            path="data/stores_uuid.csv", schema=store_schema
        ).dropDuplicates()

    def _load_csv(self, path: str, schema: str):
        """
        Loads a CSV file into a Spark DataFrame with the specified schema.

        Args:
            path (str): Path to the CSV file.
            schema (str): The schema definition for the DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the loaded data.
        """

        logger.info(f"Loading data from {path}")
        return spark.read.csv(path, header=True, schema=schema)

    def _normalize_transaction_dates(self, df: DataFrame) -> DataFrame:
        """
        Normalizes the 'transaction_date' column to a standard 'YYYY-MM-DD' format.

        Handles various date formats including:
        - YYYY-MM-DD
        - YYYY/MM/DD
        - MM/DD/YYYY
        - DD-MM-YYYY
        - Month DD, YYYY

        Args:
            df (DataFrame): Input Spark DataFrame containing transaction data.

        Returns:
            DataFrame: A Spark DataFrame with normalized transaction dates.
        """

        df = df.withColumn(
            FieldConstants.TRANSACTION_DATE,
            when(
                col(FieldConstants.TRANSACTION_DATE).rlike(
                    r"^\d{4}-\d{2}-\d{2}$"
                ),  # Matches YYYY-MM-DD
                to_date(col(FieldConstants.TRANSACTION_DATE), "yyyy-MM-dd"),
            )
            .when(
                col(FieldConstants.TRANSACTION_DATE).rlike(
                    r"^\d{4}/\d{2}/\d{2}$"
                ),  # Matches YYYY/MM/DD
                to_date(col(FieldConstants.TRANSACTION_DATE), "yyyy/MM/dd"),
            )
            .when(
                col(FieldConstants.TRANSACTION_DATE).rlike(
                    r"\d{2}\/\d{2}\/\d{4}"
                ),  # Matches MM/DD/YYYY
                to_date(col(FieldConstants.TRANSACTION_DATE), "MM/dd/yyyy"),
            )
            .when(
                col(FieldConstants.TRANSACTION_DATE).rlike(
                    r"\d{2}-\d{2}-\d{4}"
                ),  # Matches DD/MM/YYYY
                to_date(col(FieldConstants.TRANSACTION_DATE), "dd-MM-yyyy"),
            )
            .when(
                col(FieldConstants.TRANSACTION_DATE).rlike(
                    r"^[A-Za-z]+ \d{1,2}, \d{4}$"
                ),  # Matches 'Month DD, YYYY'
                to_date(col(FieldConstants.TRANSACTION_DATE), "MMMM d, yyyy"),
            )
            .otherwise(None),  # Set invalid formats to NULL
        )

        return df
