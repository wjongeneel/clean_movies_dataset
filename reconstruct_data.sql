-- After creating the database, running this query reconstructs the dataset with all relevant fields
SELECT 
	movie.rating, 
	movie.one_line,
	movie.votes,
	movie.start_year,
	movie.end_year,
	title.title,
	director.name AS director_name,
	genre.name AS genre_name,
	star.name AS star_name
FROM movie 
JOIN title ON movie.title_id = title.id
JOIN movie_to_director ON movie.id = movie_to_director.movie_id
JOIN director ON director_id = director.id
JOIN movie_to_genre ON movie.id = movie_to_genre.movie_id
JOIN genre ON genre_id = genre.id
JOIN movie_to_star ON movie.id = movie_to_star.movie_id
JOIN star ON star_id = star.id;