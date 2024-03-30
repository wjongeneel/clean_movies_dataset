import pandas as pd 
import re 

def read_csv(filename):
    try:
        return pd.read_csv(filename)
    except IOError:
        print(f'could not open file {filename}')

def normalize_column_names(df):
    df.columns = map(str.lower, df.columns)
    df.columns = df.columns.str.replace('-','_')
    df.columns = df.columns.str.replace(r'(\s+)','_', regex=True)
    return df

def rename_column(df, old_name, new_name):
    return df.rename(columns={
        old_name: new_name})

def drop_na_columns(df, max_column_null_perc):
    total_rows = df.shape[0]
    null_values_series = (df.isnull().sum()) 

    def get_columns_to_drop():
        i = 0
        columns_to_drop = list()
        for key in null_values_series.keys():
            if null_values_series.iloc[i] / total_rows > max_column_null_perc: 
                columns_to_drop.append(key)
            i += 1 
        return columns_to_drop
    
    return df.drop(labels=get_columns_to_drop(), axis=1)

def drop_rows_with_na_values(df): 
    return df.dropna().reset_index(drop=True)

def drop_duplicates(df):
    return df.drop_duplicates().reset_index(drop=True)

def extract_years(df):
    def get_years(i):
        years = re.findall('\d+', df.year.iloc[i])
        if len(years) == 2:
            return years[0], years[1]
        return years[0], None
    for i in df.index: 
        df.at[i, 'start_year'], df.at[i, 'end_year'] = get_years(i)
    df.drop(labels='year', axis=1, inplace=True)
    return df 

def strip_column(df, column_name):
    df[column_name] = df[column_name].str.strip()
    return df

def set_directors_mv(df):
    def get_directors(index):
        directors = df.stars.iloc[i].split('|')[0]
        directors = directors.strip().split('\n')[1:]
        cleaned_directors = list()
        for director in directors: 
            cleaned_directors.append(director.replace(',', '').strip())
        return cleaned_directors
    for i in df.index:
        df.at[i, 'directors'] = get_directors(i)
    return df 

def set_stars_mv(df):
    def get_stars(index):
        stars = df.stars.iloc[i].split('|')[-1]
        stars = stars.strip().split('\n')[1:]
        cleaned_stars = list() 
        for star in stars:
            cleaned_stars.append(star.replace(',', '').strip())
        return cleaned_stars
    for i in df.index: 
        df.at[i, 'stars'] = get_stars(i)
    return df

def set_genre_mv(df):
    for i in df.index:
        df.at[i, 'genre'] = df.genre.iloc[i].strip().split(',')
    return df

def mv_expand(df, column):
    return df.explode(column)

def main(csv_file):
    df = read_csv(csv_file)
    df = normalize_column_names(df)
    df = drop_na_columns(df, 0.25)
    df = drop_rows_with_na_values(df)
    df = drop_duplicates(df)
    df = extract_years(df)
    df = strip_column(df, 'one_line')
    df = set_directors_mv(df)
    df = set_stars_mv(df)
    df = set_genre_mv(df) 
    df = rename_column(df, 'movies', 'title')
    return df  

if __name__ == "__main__":
    main(csv_file)