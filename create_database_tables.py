import sqlite3
from clean_movies_dataset import main as get_dataframe

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
    movie_id    INTEGER
    PRIMARY KEY (star_id, movie_id)
);
''')


# df = get_dataframe('movies.csv')
# print(df)