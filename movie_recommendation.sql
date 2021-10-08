/*DROP TABLES IF EXIST*/

DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS hasagenre;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS taginfo;
DROP TABLE IF EXISTS genres;

/*ENTITIES*/

CREATE TABLE users ( userid INT PRIMARY KEY,
					name VARCHAR(255) NOT NULL);
					 
CREATE TABLE movies ( movieid INT PRIMARY KEY,
					title VARCHAR(255) NOT NULL);
					
CREATE TABLE taginfo ( tagid INT PRIMARY KEY,
					content VARCHAR(255) NOT NULL);
					
CREATE TABLE genres ( genreid INT PRIMARY KEY,
					name VARCHAR(255) NOT NULL);

/*RELATIONS*/

CREATE TABLE ratings ( userid INT,
					 movieid INT,
					 rating NUMERIC(2,1) CHECK (rating <= 5.0 AND rating >= 0.0),
					 timestamp BIGINT NOT NULL,
					 PRIMARY KEY (userid, movieid),
					 FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
					 FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
					 );

CREATE TABLE tags (userid INT,
				   movieid INT,
				   tagid INT,
				   timestamp BIGINT NOT NULL,
				   PRIMARY KEY (userid, movieid, tagid),
				   FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
				   FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE,
				   FOREIGN KEY (tagid) REFERENCES taginfo ON DELETE CASCADE
				  );
					 
CREATE TABLE hasagenre ( movieid INT,
						genreid INT,
						PRIMARY KEY ( movieid, genreid),
						FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE,
						FOREIGN KEY (genreid) REFERENCES genres ON DELETE CASCADE	
                     );
