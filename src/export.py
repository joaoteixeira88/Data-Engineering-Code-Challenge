from src.load_data import LoadData
from src.transformations import TransformationData
from pyspark.sql import SparkSession


class ExportData:
    def __init__(self, enriched_sales, store_revenue):
        self.spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()
        self.enriched_sales_df = enriched_sales
        self.store_revenue_df = store_revenue

    def parquet_save(self):
        (self.enriched_sales_df
         .write
         .mode("overwrite")
         .partitionBy("category", "transaction_date")
         .parquet("output/enriched_sales"))

    def csv_save(self):
        (self.store_revenue_df
         .write
         .mode("overwrite")
         .option("header", True)
         .csv("output/store_revenue"))


l = LoadData()
l.load_data()


t = TransformationData(sales=l.transactions, products=l.products, stores=l.stores)
total_revenue_df = t.calculate_total_revenue()
monthly_sales_df = t.calculate_monthly_sales()
enriched_sales_df = t.enrich_sales_data()
enriched_sales_df = t.add_price_category(enriched_sales_df)

c = ExportData(enriched_sales_df, l.stores)
c.parquet_save()
c.csv_save()
