from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, upper
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("silver").getOrCreate()

documents = spark.read.parquet("bronze/documents")

df_authors = (documents
.select(col("cover_i"), explode(col("author_name")).alias("author"))
.withColumn("author", upper(trim(col("author")))))

df_languages = (documents
.select(col("cover_i"), explode(col("language")).alias("language")))

df_books = (documents
.select(col("cover_i"), col("title"), col("edition_count"), col("first_publish_year")))

print("DataFrame de libros")
df_books.show()

print("DataFrame de idiomas")
df_languages.show()

print("DataFrame de autores")
df_authors.show()

df_books.write.mode("overwrite").parquet("silver/books")
df_languages.write.mode("overwrite").parquet("silver/languages")
df_authors.write.mode("overwrite").parquet("silver/authors")


print("Silver listo")
spark.stop()