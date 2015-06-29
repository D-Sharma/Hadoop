----------------------------
-- This pig script gets a list of year, movie with highest weight, and actors (separated by ;) with sorting on year.
-- Approach 1: First perform max weight and then join with movie dataset
---------------------------

-- Step 1 : load movie weights dataset from HDFS
movie_weight_year = LOAD 'data/movie-weights/imdb-weights.tsv' using PigStorage()
					 AS (movie:chararray, year:int, weight:float);

-- Step 2: describe the structure of the dataset
DESCRIBE movie_weight_year;

-- Step 3: Filter out records with null values
movie_weight_year = FILTER movie_weight_year BY movie IS NOT NULL and 
												year IS NOT NULL and 
												weight IS NOT NULL;

-- Step 4: Show records from movie weights relation
DESCRIBE movie_weight_year;
view_records = LIMIT movie_weight_year 5;
DUMP view_records;

-- Step 5: Find movie with maximum weight for each year
year_group = GROUP movie_weight_year BY year;
year_weight_highest = FOREACH year_group {
							sorted = ORDER movie_weight_year BY weight DESC;
							highest = LIMIT sorted 1;
							GENERATE FLATTEN(highest) AS (movie, year, weight);
};

-- Step 6: Show records for movie with highest weight for each year
show_records = LIMIT year_weight_highest 5;
DUMP show_records;


-------------------------------------------------------------
-- Step 1 : load movie weights dataset from HDFS
movie_actor_year = LOAD 'data/movie/imdb.tsv' using PigStorage()
		 AS (actor:chararray, movie:chararray, year:int);

-- Step 2: describe the structure of the dataset
DESCRIBE movie_actor_year;	

-- Step 3: Filter out records with null values 
movie_actor_year = FILTER movie_actor_year BY year is not null and 
											movie is not null and 
											actor is not null;

-- Step 4: Show records from movie actor relation
show_records = LIMIT movie_actor_year 5;
DUMP show_records;

-- Step 5: Compute a list of actors who acted in a movie separated by ';'
movie_actor_year_grouped = GROUP movie_actor_year BY (movie, year);
movie_year_all_actors = FOREACH movie_actor_year_grouped 
						GENERATE group.year as year, group.movie as movie, BagToString(movie_actor_year.actor,';') as actors;

-- Step 6: Describe structure and show records for actors acted in a movie
DESCRIBE movie_year_all_actors;						
show_records = LIMIT movie_year_all_actors 10;
DUMP show_records;

-----------------------------------------------------------
-- Join both dataset and store results
movies_join = JOIN year_weight_highest BY (movie,year), movie_year_all_actors BY (movie, year);

year_movie_weight_actors = FOREACH movies_join 
							GENERATE year_weight_highest::year AS year,
							year_weight_highest::movie AS movie, 
							 year_weight_highest::weight AS weight,
							movie_year_all_actors::actors AS actors;

sorted = ORDER year_movie_weight_actors BY year;
DESCRIBE sorted;

STORE sorted INTO 'question2_method_1_output' using PigStorage();