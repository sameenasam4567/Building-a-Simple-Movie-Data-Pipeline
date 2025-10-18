# Building-a-Simple-Movie-Data-Pipeline

## Project Overview

This project demonstrates a full **ETL (Extract, Transform, Load) pipeline** for the MovieLens “small” dataset. The main goal was to load movie and rating data into a **MySQL relational database**, while enriching it with additional information from the **OMDb API**. The enriched data includes the **director, plot summary, and box office earnings** for each movie.

After loading the data, analytical **SQL queries** were written to answer questions like which movie has the highest average rating, the top genres, and trends in movie ratings over the years.

---

## Project Structure

### Data Sources

1. **Local CSV Files**       Download Link: https://grouplens.org/datasets/movielens/latest/

   * `movies.csv` – contains movie IDs, titles, and genres.
   * `ratings.csv` – contains user ratings with timestamps.
2. **External API**

   * **OMDb API** – used to fetch additional movie details like director, plot, and box office.
   * Requires a **free API key**.    API Website: http://www.omdbapi.com/
   * Implemented a **JSON cache** to prevent repeated API calls and to handle request limits.

---

### Database

* **Relational Database:** MySQL
* **Database Name:** `movies_db`
* **User:** `movies_user`

**Tables created:**

1. **movies** – stores detailed information for each movie.
2. **ratings** – stores individual user ratings along with timestamps.
3. **genres** – stores unique genres extracted from movies.
4. **movie_genres** – a junction table linking movies to genres (many-to-many relationship).

**Constraints and Design Decisions:**

* **Primary Keys** ensure each entry is unique.
* **Foreign Keys** maintain referential integrity between tables.
* **Unique Constraints** on ratings prevent duplicate entries for the same user-movie pair.
* **ON DUPLICATE KEY UPDATE** ensures **idempotent inserts**, allowing the ETL to run multiple times without duplication.
* **ON DELETE CASCADE** ensures related data (like ratings or movie_genres) is deleted if a movie or genre is removed.

---

### Python ETL Script

* **File:** `etl.py`
* **Libraries Used:** `pandas`, `requests`, `mysql-connector-python`, `datetime`, `json`, `re`.
* **Functionality:**

  1. Reads CSV files.
  2. Cleans and parses movie titles and genres.
  3. Fetches director, plot, and box office from OMDb API.
  4. Converts timestamps to MySQL DATETIME format.
  5. Loads all data into MySQL **idempotently**:

     * Movies
     * Ratings
     * Genres
     * Movie-Genres mapping

**Challenges & Handling:**

* **API Data Issues:**

  * Sometimes the API did not return data or returned inconsistent formats.
  * Handled missing values by setting defaults like `"Data not available"`.
  * Cached API responses in a JSON file to minimize requests.

* **Duplicate Ratings:**

  * MovieLens data sometimes contains multiple ratings for the same user-movie pair.
  * Handled using `UNIQUE KEY` constraint and updating the rating on conflict.

* **Database Constraints:**

  * Ensured foreign key dependencies exist before inserting into dependent tables.
  * Used transactions and commits after each table load for data integrity.

* **Genre Parsing:**

  * Converted `|`-separated strings into a list.
  * Loaded unique genres into the `genres` table.
  * Created a many-to-many relationship between movies and genres via `movie_genres`.

---

### SQL Queries

* **File:** `queries.sql`
* **Purpose:** Answer analytical questions using the normalized database.

**Queries include:**

1. **Movie with the highest average rating**
2. **Top 5 genres by average rating**
3. **Director with the most movies**
4. **Average rating of movies by year**

**Reasoning:** Because of normalization and clean data, these queries are straightforward and efficient.

---

### Steps Followed

1. **Environment Setup**

   * Installed MySQL and started the server.
   * Created database `movies_db` and user `movies_user` with a password.
   * Tested connections using Python.

2. **Schema Definition**

   * Wrote `schema.sql` with all necessary tables, primary keys, foreign keys, and constraints.

3. **Data Extraction**

   * Loaded CSV files using pandas.
   * Cleaned and preprocessed movie titles, genres, and timestamps.
   * Fetched API data and cached results.

4. **Transformation**

   * Parsed genres, extracted year and decade from movie titles.
   * Converted timestamps and ensured all fields matched MySQL data types.

5. **Load**

   * Inserted movies, ratings, genres, and movie_genres **idempotently**.
   * Handled duplicates and foreign key constraints.

6. **Analysis**

   * Ran SQL queries to extract meaningful insights about movies, ratings, genres, and directors.

---

### Summary

This project demonstrates a complete **end-to-end ETL workflow**:

* Extracted data from **multiple sources** (CSV + API).
* **Transformed and cleaned** the data for consistency and relational storage.
* Loaded into a **normalized MySQL database** safely and idempotently.
* Prepared for **analytical queries** that answer real business questions.

**Key Takeaways:**

* Handling duplicates and API inconsistencies is crucial for a reliable ETL.
* Proper database design ensures easier analysis and data integrity.
* Caching external API responses significantly improves efficiency and reduces errors.

---

Perfect! Here’s an extended, human-style section you can add to the README, explaining **all the challenges we faced and how we solved them**, written like a real student explaining their work. You can append this at the end of your README:

---

### Challenges Faced and How They Were Handled

Working on this ETL project was a learning experience, and i faced multiple challenges along the way. Here’s a detailed walkthrough of the problems and the solutions we implemented:

#### 1. **API Issues with OMDb**

* **Problem:** While fetching data from the OMDb API, some movie titles from the CSV did not match exactly with the API database. Some API calls failed or returned incomplete data.
* **Solution:**

  * We wrote a function `clean_title()` to remove the year from movie titles to improve matching.
  * We implemented error handling in the API function to return `"Data not available"` when data was missing.
  * To reduce repeated API calls and avoid hitting the request limit, we implemented a **JSON cache** (`omdb_cache.json`) to store results. This way, once a movie’s data was fetched, it wouldn’t be requested again.

#### 2. **Handling Duplicate Ratings**

* **Problem:** The ratings CSV sometimes had multiple entries for the same user and movie combination. Simply inserting these into MySQL would violate uniqueness constraints.
* **Solution:**

  * We added a **unique key** on the combination of `user_id` and `movie_id` in the `ratings` table.
  * In the ETL script, we used `ON DUPLICATE KEY UPDATE` to update the rating and timestamp if the same user had rated the same movie multiple times.
  * This approach ensures **idempotency**: the ETL can run multiple times without creating duplicate rows.

#### 3. **Database Constraints and Foreign Keys**

* **Problem:** Tables like `ratings` and `movie_genres` depended on other tables (`movies` and `genres`) due to foreign key relationships. Inserting data in the wrong order caused errors.
* **Solution:**

  * We carefully designed the **load order**: first `movies`, then `genres`, then `movie_genres`, and finally `ratings`.
  * Used transactions and commits after each table to ensure consistency.
  * Added `ON DELETE CASCADE` to maintain referential integrity if a movie or genre is removed.

#### 4. **Parsing and Transforming Genre Data**

* **Problem:** The `genres` column in movies.csv was a single string with `|` separators (e.g., `"Adventure|Animation|Children"`). Directly loading this into MySQL was not practical for querying.
* **Solution:**

  * Parsed genres into a Python list using `.split('|')`.
  * Created a separate `genres` table to store unique genres.
  * Built a **many-to-many relationship** in `movie_genres` to map movies to multiple genres.
  * Used `INSERT IGNORE` and `ON DUPLICATE KEY UPDATE` to prevent duplicates.

#### 5. **Timestamp Conversion**

* **Problem:** Ratings timestamps in the CSV were in UNIX epoch format. Directly inserting them into MySQL caused format errors.
* **Solution:**

  * Converted timestamps in Python using `datetime.fromtimestamp()` to proper DATETIME objects before inserting into MySQL.

#### 6. **Ensuring ETL Idempotency**

* **Problem:** Running the ETL multiple times could create duplicates in `movies`, `ratings`, `genres`, or `movie_genres`.
* **Solution:**

  * Used `ON DUPLICATE KEY UPDATE` for all inserts where applicable.
  * For genres and movie-genres mapping, used `INSERT IGNORE` or `SELECT` with checks to avoid duplication.
  * This ensures the ETL is safe to rerun, which is essential for real-world pipelines.

#### 7. **General MySQL Environment Challenges**

* **Problem:** During development, large inserts or complex joins sometimes caused the MySQL server to disconnect.
* **Solution:**

  * Split inserts into smaller batches and committed after each batch.
  * Ensured that connections were properly closed after the ETL finished.
  * Used local test datasets (limited movies) to debug before scaling to the full dataset.

---

### Lessons Learned

1. **ETL pipelines need careful planning** – the order of operations matters, especially with foreign key constraints.
2. **External APIs can be unreliable**, so caching results and handling missing data is essential.
3. **Idempotency is key** – in real-life ETL, pipelines often run multiple times; preventing duplicates saves a lot of headache.
4. **Normalized databases** make analytical queries efficient but require careful mapping (like movie_genres for many-to-many relationships).
5. **Debugging in small batches** saves time and prevents data loss or MySQL server crashes.

---
   
## Submission

Prepared and executed by:
**Name: Sameena**

---


