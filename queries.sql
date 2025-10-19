-- 1. Find the movie with the highest average rating
SELECT 
    m.title, 
    AVG(r.rating) AS avg_rating
FROM 
    movie_db.movies m
JOIN 
    movie_db.ratings r ON m.movie_id = r.movie_id
GROUP BY 
    m.movie_id, m.title
ORDER BY 
    avg_rating DESC
LIMIT 1;


-- 3. Find the director who has made the most movies
SELECT 
    director, 
    COUNT(*) AS movie_count
FROM 
   movies_db.movies
GROUP BY 
    director
ORDER BY 
    movie_count DESC
LIMIT 1;


-- 4. Find the average rating of movies for each year
SELECT 
    year, 
    AVG(rating) AS avg_rating
FROM 
	movie_db.movies m
JOIN 
    ratings r ON m.movie_id = r.movie_id
GROUP BY 
    year
ORDER BY 
    year;



--- 2.What are the top 5 movie genres that have the highest average rating?
USE movie_db;

SELECT 
    m.genres AS genre, 
    AVG(r.rating) AS avg_rating
FROM 
    movies AS m
JOIN 
    ratings AS r
    ON m.movie_id = r.movie_id
GROUP BY 
    m.genres
ORDER BY 
    avg_rating DESC
LIMIT 5;

