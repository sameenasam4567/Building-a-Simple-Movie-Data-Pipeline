import pandas as pd
import mysql.connector
import re
import json
import requests
import time
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
MOVIES_CSV = r"D:\Sameena Project\tswork company\ml-latest-small\ml-latest-small\movies.csv"
RATINGS_CSV = r"D:\Sameena Project\tswork company\ml-latest-small\ml-latest-small\ratings.csv"
CACHE_FILE = r"D:\Sameena Project\tswork company\ml-latest-small\ml-latest-small\omdb_cache.json"

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sameena@123',
    'database': 'movie_db'
}

OMDB_API_KEY = "9e3bc3ff"  # replace with your key

# -----------------------------
# STEP 1: EXTRACT
# -----------------------------
movies_df = pd.read_csv(MOVIES_CSV)
ratings_df = pd.read_csv(RATINGS_CSV)

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

movie_sql = """
INSERT INTO movies (movie_id, title, genres, director, plot, box_office, year)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
title=VALUES(title), genres=VALUES(genres), director=VALUES(director),
plot=VALUES(plot), box_office=VALUES(box_office), year=VALUES(year)
"""

rating_sql = """
INSERT IGNORE INTO ratings (user_id, movie_id, rating, timestamp)
VALUES (%s, %s, %s, %s)
"""

# -----------------------------
# STEP 4: OMDb API + CACHE
# -----------------------------
# Load existing cache if available
try:
    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
        omdb_cache = json.load(f)
except FileNotFoundError:
    omdb_cache = {}

def clean_title(title):
    """Remove year from title for better API matching."""
    return re.sub(r'\(\d{4}\)', '', title).strip()

def get_movie_details(title):
    """Fetch from cache or OMDb API if not cached."""
    title_clean = clean_title(title)
    if title_clean in omdb_cache:
        data = omdb_cache[title_clean]
        return data[0], data[1], data[2]
    else:
        url = f"http://www.omdbapi.com/?t={title_clean}&apikey={OMDB_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == "True":
                director = data.get("Director", "Data not available")
                plot = data.get("Plot", "Data not available")
                box_office = data.get("BoxOffice", "Data not available")
                omdb_cache[title_clean] = [director, plot, box_office]
                # Update cache file
                with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(omdb_cache, f, indent=2)
                return director, plot, box_office
        # fallback if API fails
        omdb_cache[title_clean] = ["Data not available", "Data not available", "Data not available"]
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(omdb_cache, f, indent=2)
        return "Data not available", "Data not available", "Data not available"

# -----------------------------
# STEP 5: PROCESS MOVIES + INSERT
# -----------------------------
for _, row in movies_df.iterrows():
    title = row['title']
    genres = row['genres'] if pd.notnull(row['genres']) else None
    year_val = int(row['year']) if pd.notnull(row['year']) else None
    
    # Get director, plot, box_office from cache/API
    director, plot, box_office = get_movie_details(title)
    
    print(f"Movie: {title}")
    print(f"  Director: {director}")
    print(f"  Plot: {plot}")
    print(f"  Box Office: {box_office}\n")
    
    cursor.execute(movie_sql, (
        int(row['movie_id']),
        title,
        genres,
        director,
        plot,
        box_office,
        year_val
    ))
    conn.commit()
    time.sleep(0.1)  # avoid API overload

# -----------------------------
# STEP 6: INSERT RATINGS
# -----------------------------
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

print("\nETL finished successfully! All movies including API/cache data loaded into MySQL.")
