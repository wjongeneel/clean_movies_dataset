import sqlite3
from clean_movies_dataset import main as get_dataframe
import numpy as np

# connect to db 
conn = sqlite3.connect('movies.sqlite')
cur = conn.cursor()

# drop tables if they exist
cur.execute('DROP TABLE IF EXISTS star')
cur.execute('DROP TABLE IF EXISTS movie_to_star')
cur.execute('DROP TABLE IF EXISTS movie')
cur.execute('DROP TABLE IF EXISTS movie_to_genre')
cur.execute('DROP TABLE IF EXISTS genre')
cur.execute('DROP TABLE IF EXISTS movie_to_director')
cur.execute('DROP TABLE IF EXISTS director')
cur.execute('DROP TABLE IF EXISTS title')

# create tables
cur.execute('''
CREATE TABLE star (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);
''')

cur.execute('''
CREATE TABLE movie_to_star (
    star_id     INTEGER,
    movie_id    INTEGER,
    PRIMARY KEY (star_id, movie_id)
);
''')

cur.execute('''
CREATE TABLE movie (
    id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title_id    INTEGER NOT NULL, 
    rating      FLOAT NOT NULL,
    one_line    INTEGER NOT NULL,
    votes       INTEGER NOT NULL, 
    start_year  INTEGER NOT NULL,
    end_year    INTEGER
);
''')

cur.execute('''
CREATE TABLE movie_to_genre (
    genre_id    INTEGER,
    movie_id    INTEGER,
    PRIMARY KEY (genre_id, movie_id)
);
''')

cur.execute('''
CREATE TABLE genre (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);
''')

cur.execute('''
CREATE TABLE movie_to_director (
    director_id     INTEGER,
    movie_id        INTEGER,
    PRIMARY KEY (director_id, movie_id)
);
''')

cur.execute('''
CREATE TABLE director (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);
''')

cur.execute('''
CREATE TABLE title (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title    TEXT UNIQUE
);
''')

# function to flatten array of arrays 
def flatten_comprehension(array):
    return [item for row in array for item in row]

# read data from movies.csv 
df = get_dataframe('movies.csv')

# insert data into title db
titles = df.title.unique()
for title in titles: 
    cur.execute('INSERT OR IGNORE INTO title (title) VALUES (?);', (title,))

# insert data into genre db
genres = flatten_comprehension(list(df.genre))
genres = list(np.unique(genres))
for genre in genres: 
    cur.execute('INSERT OR IGNORE INTO genre (name) VALUES (?);', (genre,))

# insert data into star db
stars = flatten_comprehension(list(df.stars))
stars = list(np.unique(stars))
for star in stars: 
    cur.execute('INSERT OR IGNORE INTO star (name) VALUES (?);', (star,))

# insert data into director db 
directors = flatten_comprehension(list(df.directors))
directors = list(np.unique(directors))
for director in directors: 
    cur.execute('INSERT OR IGNORE INTO director (name) VALUES (?);', (director,))

# insert data into movie db, movie_to_star db, movie_to_director db, and movie_to_genre db
for i in df.index:
    # get title_id from title db
    title = df.title.iloc[i]
    cur.execute('SELECT id FROM title WHERE title = ?', (title,)) 
    title_id = cur.fetchone()[0]
    # get fields from dataframe
    rating = df.rating.iloc[i]
    one_line = df.one_line.iloc[i]
    votes = df.votes.iloc[i]
    start_year = df.start_year.iloc[i]
    end_year = df.end_year.iloc[i]
    # insert data into movie db
    cur.execute('''
    INSERT OR IGNORE INTO movie 
        (title_id, rating, one_line, votes, start_year, end_year)
        VALUES (?, ?, ?, ?, ?, ? )''', (title_id, rating, one_line, votes, start_year, end_year ) )
    # get movie_id
    cur.execute('SELECT id FROM movie WHERE rating = ? AND one_line = ? AND votes = ? AND start_year = ?', (rating, one_line, votes, start_year)) 
    movie_id = cur.fetchone()[0]
    # map star_id to movie_id in movie_to_star db 
    stars = df.stars.iloc[i]
    for star in stars:
        cur.execute('SELECT id FROM star WHERE name = ?', (star,)) 
        star_id = cur.fetchone()[0]
        cur.execute('''
            INSERT OR IGNORE INTO movie_to_star (star_id, movie_id) VALUES (?, ?)
        ''', (star_id, movie_id))
    # map director_id to movie_id in movie_to_director db
    directors = df.directors.iloc[i]
    for director in directors: 
        cur.execute('SELECT id FROM director WHERE name = ?', (director,))
        director_id = cur.fetchone()[0]
        cur.execute('''
            INSERT OR IGNORE INTO movie_to_director (director_id, movie_id) VALUES (?, ?)
        ''', (director_id, movie_id))
    # map genre_id to movie_id in movie_to_genre db 
    genres = df.genre.iloc[i]
    for genre in genres: 
        cur.execute('SELECT id FROM genre WHERE name = ?', (genre,))
        genre_id = cur.fetchone()[0]
        cur.execute('''
            INSERT OR IGNORE INTO movie_to_genre (genre_id, movie_id) VALUES (?, ?)
        ''', (genre_id, movie_id))
    
conn.commit()