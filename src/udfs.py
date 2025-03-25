from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_price(price: float) -> str:
    """Categorizes price into Low, Medium, or High"""
    if price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    else:
        return "High"

# Register the UDF with PySpark
categorize_price_udf = udf(categorize_price, StringType())