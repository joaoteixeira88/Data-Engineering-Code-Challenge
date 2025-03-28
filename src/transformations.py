from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.crosscutting.constants import FieldConstants
from src.crosscutting.logger import logger
from src.udfs import categorize_price_udf


class TransformationData:
    """
    A class responsible for transforming sales, product, and store data using PySpark.
    """

    def __init__(self, sales: DataFrame, products: DataFrame, stores: DataFrame):
        """
        Initializes the TransformationData class with sales, products, and stores DataFrames.

        Args:
            sales (DataFrame): Sales transaction data.
            products (DataFrame): Product information data.
            stores (DataFrame): Store details data.
        """

        self.sales_df = sales
        self.products_df = products
        self.stores_df = stores

    def calculate_total_revenue(self) -> DataFrame:
        """
        Calculates the total revenue per store and product category.

        Returns:
            DataFrame: A Spark DataFrame containing total revenue grouped by store and category.
        """

        logger.info("Calculating total revenue per store and category")

        # Join sales data with product data to get category information
        joined_df = self.sales_df.join(
            self.products_df,
            self.sales_df.product_id == self.products_df.product_id,
            "inner",
        )

        # Calculate total revenue (price * quantity)
        total_revenue_df = joined_df.withColumn(
            FieldConstants.TOTAL_REVENUE,
            F.col(FieldConstants.QUANTITY) * F.col(FieldConstants.PRICE),
        )

        # Group by store_id and category and calculate the sum of total_revenue
        total_revenue_df = total_revenue_df.groupBy(
            FieldConstants.STORE_ID, FieldConstants.CATEGORY
        ).agg(F.sum(FieldConstants.TOTAL_REVENUE).alias(FieldConstants.TOTAL_REVENUE))

        return total_revenue_df

    def calculate_monthly_sales(self) -> DataFrame:
        """
        Calculates monthly sales insights by aggregating total quantity sold per category.

        Returns:
            DataFrame: A Spark DataFrame containing total quantity sold grouped by year, month, and category.
        """

        logger.info("Calculating monthly sales insights")

        # Join sales data with product data to get category information
        joined_df = self.sales_df.join(
            self.products_df,
            self.sales_df.product_id == self.products_df.product_id,
            "inner",
        )

        # Extract year and month from the transaction_date column
        monthly_sales_df = joined_df.withColumn(
            FieldConstants.YEAR, F.year(FieldConstants.TRANSACTION_DATE)
        ).withColumn(FieldConstants.MONTH, F.month(FieldConstants.TRANSACTION_DATE))

        # Group by year, month, and category and calculate total quantity sold
        monthly_sales_df = monthly_sales_df.groupBy(
            FieldConstants.YEAR, FieldConstants.MONTH, FieldConstants.CATEGORY
        ).agg(F.sum(FieldConstants.QUANTITY).alias(FieldConstants.TOTAL_QUANTITY_SOLD))

        return monthly_sales_df

    def enrich_sales_data(self) -> DataFrame:
        """
        Enriches sales data by joining it with product and store details.

        Returns:
            DataFrame: A Spark DataFrame containing enriched sales data with store and product information.
        """

        logger.info("Enriching sales data with store and product details")

        # Join sales data with product and store data
        enriched_df = self.sales_df.join(
            self.products_df,
            self.sales_df.product_id == self.products_df.product_id,
            "inner",
        ).join(
            self.stores_df, self.sales_df.store_id == self.stores_df.store_id, "inner"
        )

        # Select relevant columns and return the enriched DataFrame
        enriched_df = enriched_df.select(
            FieldConstants.TRANSACTION_ID,
            FieldConstants.STORE_NAME,
            FieldConstants.LOCATION,
            FieldConstants.PRODUCT_NAME,
            FieldConstants.CATEGORY,
            FieldConstants.QUANTITY,
            FieldConstants.TRANSACTION_DATE,
            FieldConstants.PRICE,
        )

        return enriched_df

    def add_price_category(self, enriched_df):
        """
        Adds a price category column to the enriched sales DataFrame using a UDF.

        Args:
            enriched_df (DataFrame): Enriched sales DataFrame containing price information.

        Returns:
            DataFrame: A Spark DataFrame with an additional 'price_category' column.
        """

        logger.info("Adding price category column based on price")

        enriched_df = enriched_df.withColumn(
            FieldConstants.PRICE_CATEGORY,
            categorize_price_udf(enriched_df[FieldConstants.PRICE]),
        )

        return enriched_df
