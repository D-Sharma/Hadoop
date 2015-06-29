----------------------------
-- This pig script gets a list of year, movie with highest weight, and actors (separated by ;) with sorting on year.
-- Approach 2: First perform join with movie dataset and then max weight and actors are computed second.
---------------------------

--Load movies and movie_weights data
movies = LOAD 'data/movie/imdb.tsv' using PigStorage()
		 AS (actor:chararray, movie:chararray, year:int);

movie_weights = LOAD 'data/movie-weights/imdb-weights.tsv' using PigStorage()
		 AS (movie:chararray, year:int, weight:float);

-- Describe and display few records
DESCRIBE movies;
DESCRIBE movie_weights;

show_records_1 = LIMIT movies 10;
DUMP show_records_1;

show_records_2 = LIMIT movie_weights 10;
DUMP show_records_2;

--ILLUSTRATE movies;
--ILLUSTRATE movie_weights;

-- Join movie weights database with movies database
movies_join = JOIN movie_weights BY (movie,year), movies BY (movie, year);
DESCRIBE movies_join;

-- Project required columns that is year, movie, weight, actor.
movies_weights_actor = FOREACH movies_join GENERATE 
								movie_weights::year AS year,
								movie_weights::movie AS movie,
								movie_weights::weight AS weight,
								movies::actor AS actor;

-- Compute a list actors who acted in a movie separated by ';'
movies_weights_actor_group = GROUP movies_weights_actor BY (year, movie);
year_actors = FOREACH movies_weights_actor_group {
					actors = BagToString(movies_weights_actor.actor, ';');
					GENERATE group.year as year, group.movie as movie, MAX(movies_weights_actor.weight) as weight, actors as actors; 
				};

DESCRIBE year_actors;

-- For each year, compute movie with hightest weight and a list of actors who acted in the movie.
year_group = GROUP year_actors BY year;
year_max_weight = FOREACH year_group{
			sorted = ORDER year_actors BY weight DESC;
			max_weight =  LIMIT sorted 1;
			GENERATE FLATTEN(max_weight) as (year, movie, weight, actors);
			};

-- Persist the records from year_max_weight relation in file
STORE year_max_weight INTO './question2_method_2_output' using PigStorage();