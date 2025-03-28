from pyspark.sql import DataFrame

from src.crosscutting.constants import FieldConstants


class ExportData:
    """
    Handles exporting processed data to storage in Parquet and CSV formats.
    """

    def __init__(self, enriched_sales: DataFrame, store_revenue: DataFrame):
        self.enriched_sales_df = enriched_sales
        self.store_revenue_df = store_revenue

    def parquet_save(self):
        (
            self.enriched_sales_df.write.mode("overwrite")
            .partitionBy(FieldConstants.CATEGORY, FieldConstants.TRANSACTION_DATE)
            .parquet("output/enriched_sales")
        )

    def csv_save(self):
        (
            self.store_revenue_df.write.mode("overwrite")
            .option("header", True)
            .save("output/store_revenue.csv")
        )
