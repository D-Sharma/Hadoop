-- This pig script gets a list of year, movie with highest weight, and actors (separated by ;) with sorting on year.
-- Count how many movies each actor have played in and sort count in descending order.
---------------------------

--Load movies dataset
movies = LOAD 'data/movie/imdb.tsv' using PigStorage()
		 AS (actor:chararray, movie:chararray, year:int);

-- Perform data cleaning to keep records with complete values.
movies = FILTER movies BY actor is not null and 
								movie is not null and 
								year is not null;

-- Group records by actor to count number of movies each actor have played in
movies_grouped = GROUP movies BY actor;
actor_count = FOREACH movies_grouped 
				GENERATE COUNT(movies.movie) as movie_count, group as actor;

-- Sort count in descending order
actor_count_sorted = ORDER actor_count BY movie_count DESC, actor;

-- Store output in a file
STORE actor_count_sorted INTO 'question1_output' using PigStorage();

