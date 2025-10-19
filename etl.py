import pandas as pd
import mysql.connector
import re
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
MOVIES_CSV = r"D:\Sameena Project\tswork company\ml-latest-small\ml-latest-small\movies.csv"
RATINGS_CSV = r"D:\Sameena Project\tswork company\ml-latest-small\ml-latest-small\ratings.csv"

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sameena@123',
    'database': 'movie_db'
}

# -----------------------------
# STEP 1: EXTRACT
# -----------------------------
movies_df = pd.read_csv(MOVIES_CSV)
ratings_df = pd.read_csv(RATINGS_CSV)

# Rename columns to match MySQL
movies_df.rename(columns={'movieId':'movie_id'}, inplace=True)
ratings_df.rename(columns={'userId':'user_id', 'movieId':'movie_id'}, inplace=True)

print("Movies CSV preview:")
print(movies_df.head())

print("\nRatings CSV preview:")
print(ratings_df.head())

# -----------------------------
# STEP 2: TRANSFORM
# -----------------------------
def extract_year(title):
    match = re.search(r'\((\d{4})\)', str(title))
    return int(match.group(1)) if match else None

movies_df['year'] = movies_df['title'].apply(extract_year)
movies_df['decade'] = movies_df['year'].apply(lambda x: x - x % 10 if x else None)
ratings_df['timestamp_dt'] = ratings_df['timestamp'].apply(lambda x: datetime.fromtimestamp(x))

print("\nMovies after transformation (year + decade):")
print(movies_df.head())

print("\nRatings after transformation (timestamp converted):")
print(ratings_df.head())

# -----------------------------
# STEP 3: LOAD TO MYSQL
# -----------------------------
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Movies SQL
movie_sql = """
INSERT INTO movies (movie_id, title, genres, director, plot, box_office, year)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
title=VALUES(title), genres=VALUES(genres), director=VALUES(director),
plot=VALUES(plot), box_office=VALUES(box_office), year=VALUES(year)
"""

# Prepare data for executemany
movie_data = []
for _, row in movies_df.iterrows():
    year_val = int(row['year']) if pd.notnull(row['year']) else None
    movie_data.append((
        int(row['movie_id']),
        row['title'],
        row['genres'] if pd.notnull(row['genres']) else None,
        "Data not available",  # director
        "Data not available",  # plot
        "Data not available",  # box_office
        year_val
    ))

cursor.executemany(movie_sql, movie_data)
conn.commit()

# Ratings SQL
rating_sql = """
INSERT IGNORE INTO ratings (user_id, movie_id, rating, timestamp)
VALUES (%s, %s, %s, %s)
"""

rating_data = []
for _, row in ratings_df.iterrows():
    rating_data.append((
        int(row['user_id']),
        int(row['movie_id']),
        float(row['rating']),
        int(row['timestamp'])
    ))

cursor.executemany(rating_sql, rating_data)
conn.commit()

cursor.close()
conn.close()

print("\nETL finished successfully! Data loaded into MySQL.")


