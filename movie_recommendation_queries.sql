DROP TABLE IF EXISTS query1;

/* SQL query to return the total number of movies for each genre */

CREATE TABLE query1 AS 
SELECT name, COUNT(h.movieid) AS moviecount
FROM hasagenre h
INNER JOIN genres g ON g.genreid = h.genreid
GROUP BY g.name;

/* SQL query to return the average rating per genre */

DROP TABLE IF EXISTS query2;

CREATE TABLE query2 AS 
SELECT name, AVG(r.rating) AS rating
FROM ratings r
INNER JOIN hasagenre h ON r.movieid = h.movieid
INNER JOIN genres g ON g.genreid = h.genreid
GROUP BY g.name;

/* SQL query to return the movies have at least 10 ratings */

DROP TABLE IF EXISTS query3;

CREATE TABLE query3 AS
SELECT * FROM
(SELECT title, COUNT(r.movieid) AS countofratings
FROM ratings r
INNER JOIN movies m ON r.movieid = m.movieid
GROUP BY m.title) temp
WHERE temp.countofratings >= 10;

/* SQL query to return all “Comedy” movies, including movieid and title */

DROP TABLE IF EXISTS query4;

CREATE TABLE query4 AS
SELECT m.movieid, title
FROM movies m, genres g, hasagenre h
WHERE m.movieid = h.movieid AND h.genreid = g.genreid AND g.name = 'Comedy';

/* SQL query to return the average rating per movie */

DROP TABLE IF EXISTS query5;

CREATE TABLE query5 AS
SELECT title, AVG(r.rating) AS average
FROM ratings r, movies m
WHERE r.movieid = m.movieid
GROUP BY m.title;

/* SQL query to return the average rating for all “Comedy” movies */

DROP TABLE IF EXISTS query6;

CREATE TABLE query6 AS
SELECT AVG(r.rating) AS average
FROM ratings r, query4 c
WHERE r.movieid = c.movieid;

/* SQL query to return the average rating for all movies and each of these movies is both “Comedy” and “Romance”. */

DROP TABLE IF EXISTS query7;

CREATE TABLE query7 AS
SELECT AVG(r.rating) as average
FROM ratings r, hasagenre h, genres g
WHERE r.movieid = h.movieid AND h.genreid = g.genreid AND g.name = 'Romance' AND r.movieid IN (SELECT r2.movieid
																							  FROM ratings r2, hasagenre h2, genres g2
																							  WHERE r2.movieid = h2.movieid AND h2.genreid = g2.genreid AND g2.name = 'Comedy');
																							  
/*SQL query to return the average rating for all movies and each of these movies is “Romance” but not “Comedy”. */

DROP TABLE IF EXISTS query8;

CREATE TABLE query8 AS
SELECT AVG(r.rating) as average
FROM ratings r, hasagenre h, genres g
WHERE r.movieid = h.movieid AND h.genreid = g.genreid AND g.name = 'Romance' AND r.movieid NOT IN (SELECT r2.movieid
																							  FROM ratings r2, hasagenre h2, genres g2
																							  WHERE r2.movieid = h2.movieid AND h2.genreid = g2.genreid AND g2.name = 'Comedy');
											
											
																							  
/* SQL query to find movies rated by a user*/				

DROP TABLE IF EXISTS query9;

CREATE TABLE query9 AS
SELECT r.movieid AS movieid, r.rating AS rating
FROM ratings r
WHERE r.userid = :v1;
