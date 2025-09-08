import requests 
import pyodbc
import json
import pandas as pd
import time
import re
import numpy as np

from config import OMDBKEY, DB_SERVER, DB_DATABASE

MOVIES_JSON_PATH = "movies.json"
MOVIE_DETAILS_PATH = "movieDetails.csv"

def load_movies_json():
    with open(MOVIES_JSON_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_movie_details_csv():
    df = pd.read_csv(MOVIE_DETAILS_PATH)
    df['Title'] = df['Title'].str.strip().str.lower()
    lookup = {
        title: {
            'rating': row['Rating'] if pd.notna(row['Rating']) else None,
            'factoid': row.get('Factoid')
        }
        for _, row in df.iterrows()
        if pd.notna(row['Title'])
        for title in [row['Title']]
    }
    return lookup

def extract_movie_data(title):
    API_URL = f"http://www.omdbapi.com/?t={title}&apikey={OMDBKEY}"
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching {title}: {e}")
    return None

def format_movie_data(raw_data, studio, factoid=None, rating=None):
    return {
        "name": raw_data.get("Title"),
        "rating": rating,
        "plot": raw_data.get("Plot"),
        "director": raw_data.get("Director"),
        "studio": studio,
        "factoid": factoid
    }

def format_errors(error_titles):
    return pd.DataFrame([{
        "name": title,
        "rating": None,
        "plot": None,
        "director": None,
        "studio": None,
        "factoid": None
    } for title in error_titles])

def find_best_match(title_key, lookup_keys):
    
    if title_key in lookup_keys:
        return title_key
    
    for key in lookup_keys:
        if title_key in key or key in title_key:
            return key

    return None

def main():
    movies_list = load_movies_json()
    movie_details_lookup = load_movie_details_csv()

    all_movies_data = []
    error_movies = []

    for movie in movies_list:
        title = movie.get('title', '').strip()
        title_key = title.lower()
        matched_key = find_best_match(title_key, movie_details_lookup.keys())
        raw_data = extract_movie_data(title)
        if not raw_data or raw_data.get("Response") == "False":
            error_movies.append(title)
            continue
        csv_data = movie_details_lookup.get(matched_key, {})
        formatted = format_movie_data(raw_data, movie.get('studio'), csv_data.get('factoid'), csv_data.get('rating'))
        all_movies_data.append(formatted)

    valid_df = pd.DataFrame(all_movies_data)
    error_df = format_errors(error_movies)
    full_df = pd.concat([valid_df, error_df], ignore_index=True)

    upload_sql(full_df)
    print(full_df)

def upload_sql(df):
    conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    valid_df = df[(df['plot'].notna()) & (df['plot'] != "N/A")]
    error_df = df[(df['plot'].isna()) | (df['plot'] == "N/A")]

    # Insert error movies
    if not error_df.empty:
        cursor.executemany(
            "INSERT INTO error_movies (Title) VALUES (?)",
            [(row['name'],) for _, row in error_df.iterrows()]
        )

    unique_pairs = valid_df[['director', 'studio']].dropna().drop_duplicates()
    pair_id_map = {}
    for _, row in unique_pairs.iterrows():
        director_name = row['director'].strip()
        studio_name = row['studio'].strip()

        # Insert studio 
        cursor.execute("""
            INSERT INTO studio (Name) OUTPUT INSERTED.StudioID
            VALUES (?)
        """, studio_name)
        studio_id = cursor.fetchone()[0]

        # Insert director
        cursor.execute("""
            INSERT INTO director (Name, StudioID) OUTPUT INSERTED.DirectorID
            VALUES (?, ?)
        """, director_name, studio_id)
        director_id = cursor.fetchone()[0]

        pair_id_map[(director_name, studio_name)] = director_id

    # Insert movies 
    for _, row in valid_df.iterrows():
        director_name = row['director'].strip()
        studio_name = row['studio'].strip()
        director_id = pair_id_map.get((director_name, studio_name))

        if director_id is None:
            print(f"Warning: No director-studio pair found for movie '{row['name']}'. Skipping insert.")
            continue

        try:
            rating = float(row['rating']) if pd.notna(row['rating']) else None
        except (ValueError, TypeError):
            rating = None

        factoid = row['factoid'] if pd.notna(row['factoid']) else None

        cursor.execute("""
            INSERT INTO movies (Title, Plot, Rating, Factoid, DirectorID)
            VALUES (?, ?, ?, ?, ?)
        """, row['name'], row['plot'], rating, factoid, director_id)

    conn.commit()
    cursor.close()
    conn.close()
main()
