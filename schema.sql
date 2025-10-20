-- schema.sql Sameena

DROP DATABASE IF EXISTS movie_db;
CREATE DATABASE IF NOT EXISTS movie_db;
USE movie_db;

DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies;

CREATE TABLE IF NOT EXISTS movies (
    movie_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    genres VARCHAR(255),
    director VARCHAR(255),
    plot TEXT,
    box_office VARCHAR(50),
    year INT
);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    movie_id INT NOT NULL,
    rating FLOAT,
    timestamp BIGINT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
);




