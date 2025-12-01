import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()

URL = "https://openlibrary.org/search.json?q=python"

response = requests.get(URL).json()
products = response["docs"]

data = []

for p in products:
    data.append((p.get("id"), p.get("title"), p.get("price"), p.get("rating"), p.get("brand"), p.get("category")))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("price", DoubleType()),
    StructField("rating", DoubleType()),
    StructField("brand", StringType()),
    StructField("category", StringType())
])

df = spark.createDataFrame(data, schema)

# =======================================================
# 3) GUARDAR BRONZE
# =======================================================

df.write.mode("overwrite").parquet("bronze/products")
df.show()

print("BRONZE listo.")

spark.stop()