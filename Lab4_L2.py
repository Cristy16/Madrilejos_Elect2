"""
ELEC2 Lab Activity - PySpark Data Processing & Visualization
Group Repository: Madrilejos_Elec2
Filename: Lab2.py
"""

import os
import sys

# 1. HADOOP PATH FIX (Dapat ito ang pinaka-una)
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PATH'] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, desc, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# ============================================
# PART 1: INITIALIZATION AND DATA LOADING
# ============================================

CSV_FILE_PATH = r"C:\Users\USER\Documents\3rd Year 2nd Sem\BDA\simple_kaggle_dataset.csv" 

spark = SparkSession.builder \
    .appName("ELEC2_Lab4_DataProcessing") \
    .getOrCreate()

print("\nSpark Session Created Successfully")

schema = StructType([
    StructField("Product_ID", IntegerType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Quantity_Sold", IntegerType(), True),
    StructField("Rating", FloatType(), True)
])

# Load dataset
df = spark.read.option("header", "true").schema(schema).csv(CSV_FILE_PATH)
df.show(10)

# ============================================
# PART 2: DATA PARTITIONING STRATEGIES
# ============================================
print("\n--- PARTITIONING STRATEGIES ---")

# Strategy 1: Hash Partitioning by Category
hash_partitioned = df.repartition(4, "Category")
print(f"Partitions after hash partitioning: {hash_partitioned.rdd.getNumPartitions()}")

# Strategy 2: Range Partitioning by Price
range_partitioned = df.repartitionByRange(3, "Price")
print(f"Partitions after range partitioning: {range_partitioned.rdd.getNumPartitions()}")

# ============================================
# PART 3: TRANSFORMATION PIPELINE
# ============================================
print("\n--- TRANSFORMATION PIPELINE ---")

# Add Revenue and Rating Status
df_transformed = df.withColumn("Total_Revenue", col("Price") * col("Quantity_Sold")) \
                   .withColumn("Rating_Status", when(col("Rating") >= 4.0, "High").otherwise("Standard"))

# Aggregation
category_stats = df_transformed.groupBy("Category").agg(
    count("Product_ID").alias("Product_Count"),
    avg("Price").alias("Average_Price"),
    sum("Total_Revenue").alias("Total_Revenue")
).orderBy(desc("Total_Revenue"))

category_stats.show()

# ============================================
# PART 4: SAVE RESULTS (WINDOWS-SAFE VERSION)
# ============================================
output_folder = "lab4_output"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

print("\nSaving results to folder...")

try:
    pandas_df = category_stats.toPandas()
    pandas_df.to_csv(os.path.join(output_folder, "category_stats.csv"), index=False)
    print(f"Success! CSV file created at: {output_folder}/category_stats.csv")
except Exception as e:
    print(f"Pandas save failed: {e}")
    category_stats.coalesce(1).write.mode("overwrite").csv(f"{output_folder}/spark_category_stats", header=True)

# ============================================
# PART 5: VISUALIZATIONS (MATPLOTLIB + SEABORN)
# ============================================

import matplotlib.pyplot as plt
import seaborn as sns

# Consistent color palette
colors = sns.color_palette("Set2", len(pandas_df))

# ---------- Matplotlib Visualizations ----------
plt.figure(figsize=(16,12))

# 1. Total Revenue per Category
plt.subplot(2,2,1)
plt.bar(pandas_df['Category'], pandas_df['Total_Revenue'], color=colors)
plt.title("Total Revenue by Category")
plt.xlabel("Category")
plt.ylabel("Total Revenue")
plt.xticks(rotation=45)

# 2. Product Count per Category
plt.subplot(2,2,2)
plt.barh(pandas_df['Category'], pandas_df['Product_Count'], color=colors)
plt.title("Product Count by Category")
plt.xlabel("Number of Products")
plt.ylabel("Category")

# 3. Average Price vs Total Revenue
plt.subplot(2,2,3)
plt.scatter(pandas_df['Average_Price'], pandas_df['Total_Revenue'], color='purple', s=100, alpha=0.6)
for i, txt in enumerate(pandas_df['Category']):
    plt.annotate(txt, (pandas_df['Average_Price'][i], pandas_df['Total_Revenue'][i]), xytext=(5,5), textcoords='offset points')
plt.title("Average Price vs Total Revenue")
plt.xlabel("Average Price")
plt.ylabel("Total Revenue")

# 4. Revenue Distribution Pie
plt.subplot(2,2,4)
plt.pie(pandas_df['Total_Revenue'], labels=pandas_df['Category'], autopct='%1.1f%%', colors=colors)
plt.title("Revenue Distribution by Category")

plt.tight_layout()
plt.show()

# ---------- Seaborn Visualizations ----------
plt.figure(figsize=(16,12))

# 1. Barplot: Total Revenue
plt.subplot(2,2,1)
sns.barplot(data=pandas_df, x='Category', y='Total_Revenue', palette="Set2")
plt.title("Total Revenue by Category (Seaborn)")
plt.xticks(rotation=45)

# 2. Barplot: Product Count
plt.subplot(2,2,2)
sns.barplot(data=pandas_df, x='Category', y='Product_Count', palette="Set2")
plt.title("Product Count by Category (Seaborn)")
plt.xticks(rotation=45)

# 3. Scatter + Regression: Avg Price vs Revenue
plt.subplot(2,2,3)
sns.regplot(data=pandas_df, x='Average_Price', y='Total_Revenue', scatter_kws={'s':80, 'color':'orange'}, line_kws={'color':'blue'})
plt.title("Average Price vs Total Revenue (Seaborn)")

# 4. Heatmap: Correlation of numeric columns
plt.subplot(2,2,4)
sns.heatmap(pandas_df[['Product_Count','Average_Price','Total_Revenue']].corr(), annot=True, cmap='coolwarm', linewidths=0.5)
plt.title("Correlation Heatmap")

plt.tight_layout()
plt.show()

print("\nLab Activity Completed Successfully")
spark.stop()