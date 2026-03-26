# ================================
# Laboratory 3
# Netflix Visualizations
# ================================

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load CSV
file_path = r"C:\Users\USER\Documents\3rd Year 2nd Sem\BDA\netflix_titles.csv"
df = pd.read_csv(file_path)

# Clean 'listed_in' for genre analysis
df['listed_in'] = df['listed_in'].fillna('Unknown')

# ================================
# Matplotlib Visualizations
# ================================

plt.style.use('ggplot')

# 1. Count of Movies vs TV Shows
plt.figure(figsize=(6,4))
colors = ['#1f77b4', '#ff7f0e']
df['type'].value_counts().plot(kind='bar', color=colors)
plt.title("Count of Movies vs TV Shows", fontsize=14, fontweight='bold')
plt.ylabel("Count", fontsize=12)
plt.xticks(rotation=0, fontsize=11)
plt.show()

# 2. Top 10 Countries with Most Shows
top_countries = df['country'].value_counts().head(10)
plt.figure(figsize=(8,5))
top_countries.sort_values().plot(kind='barh', color='#2ca02c')
plt.title("Top 10 Countries with Most Shows", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Country", fontsize=12)
plt.show()

# 3. Distribution of Shows by Release Year
plt.figure(figsize=(10,4))
df['release_year'].value_counts().sort_index().plot(kind='line', color='#d62728', linewidth=2)
plt.title("Distribution of Shows by Release Year", fontsize=14, fontweight='bold')
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Shows", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.show()

# 4. Rating Distribution
plt.figure(figsize=(10,5))
df['rating'].value_counts().sort_values(ascending=True).plot(kind='barh', color='#9467bd')
plt.title("Show Ratings Distribution", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Rating", fontsize=12)
plt.show()

# 5. Top 10 Genres
all_genres = df['listed_in'].str.split(', ')
genres_series = pd.Series([genre.strip() for sublist in all_genres for genre in sublist])
top_genres = genres_series.value_counts().head(10)
plt.figure(figsize=(10,5))
top_genres.sort_values().plot(kind='barh', color='#17becf')
plt.title("Top 10 Genres on Netflix", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Genre", fontsize=12)
plt.show()

# ================================
# Seaborn Visualizations
# ================================

sns.set(style="whitegrid")

# 1. Count of Movies vs TV Shows
plt.figure(figsize=(6,4))
sns.countplot(data=df, x='type', palette='pastel')
plt.title("Count of Movies vs TV Shows", fontsize=14, fontweight='bold')
plt.show()

# 2. Top 10 Countries with Most Shows
plt.figure(figsize=(10,5))
sns.countplot(
    data=df[df['country'].isin(df['country'].value_counts().head(10).index)],
    y='country',
    order=df['country'].value_counts().head(10).index,
    palette='magma'
)
plt.title("Top 10 Countries with Most Shows", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Country", fontsize=12)
plt.show()

# 3. Rating Distribution
plt.figure(figsize=(10,5))
sns.countplot(data=df, y='rating', order=df['rating'].value_counts().index, palette='coolwarm')
plt.title("Show Ratings Distribution", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Rating", fontsize=12)
plt.show()

# 4. Release Year Distribution
plt.figure(figsize=(12,4))
sns.histplot(data=df, x='release_year', bins=30, kde=True, color='#2ca02c')
plt.title("Distribution of Shows by Release Year", fontsize=14, fontweight='bold')
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Shows", fontsize=12)
plt.show()

# 5. Top 10 Genres
plt.figure(figsize=(10,5))
sns.barplot(x=top_genres.values, y=top_genres.index, palette='viridis')
plt.title("Top 10 Genres on Netflix", fontsize=14, fontweight='bold')
plt.xlabel("Count", fontsize=12)
plt.ylabel("Genre", fontsize=12)
plt.show()