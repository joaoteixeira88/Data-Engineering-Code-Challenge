from src.export import ExportData
from src.load_data import LoadData
from src.transformations import TransformationData

load = LoadData()
load.load_data()


t = TransformationData(
    sales=load.transactions, products=load.products, stores=load.stores
)
total_revenue_df = t.calculate_total_revenue()
monthly_sales_df = t.calculate_monthly_sales()
enriched_sales_df = t.enrich_sales_data()
enriched_sales_df = t.add_price_category(enriched_sales_df)

c = ExportData(enriched_sales_df, load.stores)
c.parquet_save()
c.csv_save()
