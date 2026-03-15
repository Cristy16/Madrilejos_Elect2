# Lab#.py
# Group Lab Activity - PySpark Netflix Dataset
# Members: Borjal D., Baraquiel, Bonaobra, Madrilejos

# ------------------------
# Step 1: Import Libraries
# ------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, regexp_extract

# ------------------------
# Step 2: Create Spark Session
# ------------------------
spark = SparkSession.builder.appName("Lab3").getOrCreate()

# ------------------------
# Step 3: Load CSV Dataset
# ------------------------
df = spark.read.csv(r"C:\Users\USER\Downloads\netflix_titles.csv", header=True, inferSchema=True)

# ------------------------
# Step 4: Inspect DataFrame
# ------------------------
df.printSchema()
df.describe().show()
df.show(5)

# ------------------------
# Step 5: Clean Dataset
# ------------------------
# Remove duplicate rows
df = df.dropDuplicates()

# Fill missing values for 'director' and 'country'
df = df.fillna({"director": "Unknown", "country": "Unknown"})

# ------------------------
# Step 6: Basic DataFrame Operations
# ------------------------
# 6.1 Select specific columns
df.select("title", "type", "release_year", "listed_in").show(5)

# 6.2 Filter rows
df.filter((df.type == "Movie") & (df.release_year > 2020)).show(5)
df.filter((df.type == "TV Show") & (df.rating == "TV-MA")).show(5)

# 6.3 Transform columns
# Extract numeric duration in minutes
df = df.withColumn("minutes", regexp_extract("duration", "(\d+)", 1).cast("int"))
df.show(5)

# ------------------------
# Step 7: Register DataFrame as SQL Table
# ------------------------
df.createOrReplaceTempView("netflix")

# Example SQL query: top 5 most frequent categories
top_categories = df.withColumn("category", explode(split(col("listed_in"), ", "))) \
                   .groupBy("category") \
                   .agg(count("*").alias("count")) \
                   .orderBy(col("count").desc())

top_categories.show(5)

# ------------------------
# Step 8: Export Results
# ------------------------
# CSV
top_categories.toPandas().to_csv("C:/Users/USER/Downloads/top_categories.csv", index=False)

#TXT export
top_categories.toPandas().to_csv("C:/Users/USER/Downloads/top_categories.txt", sep="\t", index=False)

#JSON export
top_categories.write.mode("overwrite").json("C:/Users/USER/Downloads/top_categories_json")

print("Lab completed successfully!")