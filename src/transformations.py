from src.crosscutting.logger import logger
from pyspark.sql import functions as F, SparkSession
from pyspark.sql import DataFrame

from src.load_data import LoadData
from src.udfs import categorize_price_udf


class TransformationData:
    def __init__(self, sales, products, stores):
        self.spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()
        self.sales_df = sales
        self.products_df = products
        self.stores_df = stores

    def calculate_total_revenue(self) -> DataFrame:
        """
        Calculate total revenue per store and product category.

        Args:
            sales_df: DataFrame containing sales data
            products_df: DataFrame containing product data

        Returns:
            DataFrame with columns: store_id, category, total_revenue
        """
        logger.info("Calculating total revenue per store and category")

        # Join sales data with product data to get category information
        joined_df = self.sales_df.join(
            self.products_df, self.sales_df.product_id == self.products_df.product_id, "inner"
        )

        # Calculate total revenue (price * quantity)
        total_revenue_df = joined_df.withColumn(
            "total_revenue", F.col("quantity") * F.col("price")
        )

        # Group by store_id and category and calculate the sum of total_revenue
        total_revenue_df = total_revenue_df.groupBy("store_id", "category").agg(
            F.sum("total_revenue").alias("total_revenue")
        )

        return total_revenue_df


    def calculate_monthly_sales(self) -> DataFrame:
        """
        Calculate total quantity sold per product category grouped by month.

        Args:
            sales_df: DataFrame containing sales data
            products_df: DataFrame containing product data

        Returns:
            DataFrame with columns: year, month, category, total_quantity_sold
        """
        logger.info("Calculating monthly sales insights")

        # Join sales data with product data to get category information
        joined_df = self.sales_df.join(
            self.products_df, self.sales_df.product_id == self.products_df.product_id, "inner"
        )

        # Extract year and month from the transaction_date column
        monthly_sales_df = joined_df.withColumn(
            "year", F.year("transaction_date")
        ).withColumn(
            "month", F.month("transaction_date")
        )

        # Group by year, month, and category and calculate total quantity sold
        monthly_sales_df = monthly_sales_df.groupBy("year", "month", "category").agg(
            F.sum("quantity").alias("total_quantity_sold")
        )

        return monthly_sales_df

    def enrich_sales_data(self) -> DataFrame:
        """
        Enrich the sales data by combining it with the products and stores datasets.

        Args:
            sales_df: DataFrame containing sales data
            products_df: DataFrame containing product data
            stores_df: DataFrame containing store data

        Returns:
            DataFrame with enriched sales data
        """
        logger.info("Enriching sales data with store and product details")

        # Join sales data with product and store data
        enriched_df = self.sales_df.join(
            self.products_df, self.sales_df.product_id == self.products_df.product_id, "inner"
        ).join(
            self.stores_df, self.sales_df.store_id == self.stores_df.store_id, "inner"
        )

        # Select relevant columns and return the enriched DataFrame
        enriched_df = enriched_df.select(
            "transaction_id", "store_name", "location", "product_name",
            "category", "quantity", "transaction_date", "price"
        )

        return enriched_df

    def add_price_category(self, enriched_df):
        """
        Adds a price_category column to the enriched dataset.

        Args:
            enriched_df (DataFrame): The enriched DataFrame containing sales, store, and product data.

        Returns:
            DataFrame: DataFrame with an additional price_category column.
        """
        logger.info("Adding price category column based on price")

        enriched_df = enriched_df.withColumn("price_category", categorize_price_udf(enriched_df["price"]))

        return enriched_df