from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_date

from src.crosscutting.logger import logger
from src.data_models.schemas import transaction_schema, store_schema, product_schema


class LoadData:
    def __init__(self):
        self.spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()
        self.products = None
        self.transactions = None
        self.stores = None

    def load_data(self):
        self.transactions = self._load_csv(path="data/sales_uuid.csv", schema=transaction_schema)
        self.transactions = self._normalize_transaction_dates(df=self.transactions)
        self.transactions = self.transactions.filter(self.transactions.quantity>0).dropDuplicates()

        self.products = self._load_csv(path="data/products_uuid.csv", schema=product_schema).dropDuplicates()
        self.stores = self._load_csv(path="data/stores_uuid.csv", schema=store_schema).dropDuplicates()

    def _load_csv(self, path: str, schema: str):
        logger.info(f"Loading data from {path}")
        return self.spark.read.csv(path, header=True, schema=schema)

    def _normalize_transaction_dates(self, df):
        """
        Converts different date formats in 'transaction_date' to 'YYYY-MM-DD'
        """

        df = df.withColumn(
            "transaction_date",
            when(
                col("transaction_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),  # Matches YYYY-MM-DD
                to_date(col("transaction_date"), "yyyy-MM-dd")
            ).when(
                col("transaction_date").rlike(r"^\d{4}/\d{2}/\d{2}$"),  # Matches YYYY/MM/DD
                to_date(col("transaction_date"), "yyyy/MM/dd")
            ).when(
                col("transaction_date").rlike(r"\d{2}\/\d{2}\/\d{4}"),  # Matches YYYY/MM/DD
                to_date(col("transaction_date"), "MM/dd/yyyy")
            ).when(
                col("transaction_date").rlike(r"\d{2}-\d{2}-\d{4}"),  # Matches YYYY/MM/DD
                to_date(col("transaction_date"), "dd-MM-yyyy")
            ).when(
                col("transaction_date").rlike(r"^[A-Za-z]+ \d{1,2}, \d{4}$"),  # Matches 'Month DD, YYYY'
                to_date(col("transaction_date"), "MMMM d, yyyy")
            ).otherwise(None)  # Set invalid formats to NULL
        )

        return df
