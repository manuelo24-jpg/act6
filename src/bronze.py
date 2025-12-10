import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()

URL = "https://openlibrary.org/search.json?q=python"

response = requests.get(URL).json()
documents = response["docs"]

data = []

for document in documents:
    data.append((document.get("author_key" ), document.get("author_name"),
                 document.get("edition_count"), document.get("first_publish_year"),
                 document.get("language"), document.get("title"), document.get("cover_i")))

schema = StructType([
    StructField("author_key", StringType(), True),
    StructField("author_name", ArrayType(StringType()), True),
    StructField("edition_count", IntegerType(), True),
    StructField("first_publish_year", IntegerType(), True),
    StructField("language", ArrayType(StringType()), True),
    StructField("title", StringType(), True),
    StructField("cover_i", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.write.mode("overwrite").parquet("bronze/documents")
df.show()

print("BRONZE listo.")

spark.stop()