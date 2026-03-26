# Lab1_RDD_Pipeline.py
# Name: Madrilejos, Polly Cristy P.
#       Bonaobra, Ian Raphael
#       Borjal, Daryl James
#       Baraquiel, Danilyn Krixtin 
# Lab No. 1
# Date: 02/14/2026
# Objective: Implement a distributed data processing pipeline using Apache Spark RDDs
#            and create visualizations using Matplotlib and Seaborn

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RDD Pipeline Activity") \
    .getOrCreate()

# Get the SparkContext
sc = spark.sparkContext

# List of sentences (input data)
sentences = [
    "Spark makes big data processing fast",
    "RDD operations are powerful",
    "Spark uses lazy evaluation"
]

# Step 1: Create an initial RDD
rdd = sc.parallelize(sentences)

# Step 2: Split sentences into words (flatMap)
words_rdd = rdd.flatMap(lambda sentence: sentence.split(" "))

# Step 3: Convert words to lowercase (map)
lowercase_rdd = words_rdd.map(lambda word: word.lower())

# Step 4: Filter out short words (length <= 2)
filtered_rdd = lowercase_rdd.filter(lambda word: len(word) > 2)

# Step 5: Remove duplicate words (distinct)
unique_rdd = filtered_rdd.distinct()

# Step 6: Sort words alphabetically (sortBy)
sorted_rdd = unique_rdd.sortBy(lambda word: word)

# Step 7: Collect results to driver
final_result = sorted_rdd.collect()

# Convert result to DataFrame
df = pd.DataFrame(final_result, columns=['word'])

# Add word length column
df['length'] = df['word'].apply(len)

print("DataFrame:")
print(df)

# Print the final sorted unique words
print("\nFinal Result:")
print(final_result)

# ==============================
# MATPLOTLIB VISUALIZATIONS
# ==============================

# 1 Bar Chart
plt.figure()
plt.bar(df['word'], df['length'])
plt.title("Word Length Distribution")
plt.xlabel("Words")
plt.ylabel("Length")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 2 Histogram
plt.figure()
plt.hist(df['length'], bins=5)
plt.title("Frequency of Word Lengths")
plt.xlabel("Word Length")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

# 3 Line Plot
plt.figure()
plt.plot(df['word'], df['length'], marker='o')
plt.title("Word Length Trend")
plt.xlabel("Words")
plt.ylabel("Length")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 4 Scatter Plot
plt.figure()
plt.scatter(range(len(df)), df['length'])
plt.title("Word Index vs Word Length")
plt.xlabel("Word Index")
plt.ylabel("Word Length")
plt.tight_layout()
plt.show()

# 5 Pie Chart
plt.figure()
plt.pie(df['length'], labels=df['word'], autopct='%1.1f%%')
plt.title("Proportion of Word Lengths")
plt.tight_layout()
plt.show()

# ==============================
# SEABORN VISUALIZATIONS
# ==============================

# 6 Seaborn Barplot
plt.figure()
sns.barplot(x='word', y='length', data=df)
plt.title("Word Length by Word")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 7 Seaborn Histogram
plt.figure()
sns.histplot(df['length'], kde=True)
plt.title("Word Length Distribution (Seaborn)")
plt.tight_layout()
plt.show()

# 8 Seaborn Boxplot
plt.figure()
sns.boxplot(y=df['length'])
plt.title("Boxplot of Word Length")
plt.tight_layout()
plt.show()

# 9 Seaborn Scatterplot
plt.figure()
sns.scatterplot(x=df.index, y='length', data=df)
plt.title("Word Index vs Length (Seaborn)")
plt.xlabel("Word Index")
plt.ylabel("Length")
plt.tight_layout()
plt.show()

# 10 Seaborn Countplot
plt.figure()
sns.countplot(x='length', data=df)
plt.title("Count of Word Lengths")
plt.xlabel("Word Length")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# Stop SparkSession
spark.stop()