import requests
import pyodbc
import json
import pandas as pd
import time
import re

from config import OMDBKEY, DB_SERVER, DB_DATABASE

def connect():
    return pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + DB_SERVER + ";DATABASE=" + DB_DATABASE + ";Trusted_Connection=yes;")

def get_rotten_tomatoes_rating(row, api_data):
    rt_rating = row.get('Rotten Tomatoes Score')
    if rt_rating and not pd.isna(rt_rating):
        try:
            return int(re.sub(r'[^\d]', '', str(rt_rating)))
        except:
            pass
    if api_data:
        for rating in api_data.get("Ratings", []):
            if rating["Source"] == "Rotten Tomatoes":
                try:
                    return int(rating["Value"].replace("%", ""))
                except:
                    pass
    return None

def main():
    conn = connect()
    cursor = conn.cursor()

    try:
        movies_df = pd.read_json("movies.json")
    except:
        print("Error loading movies.json")
        return

    try:
        movie_details_df = pd.read_csv("movieDetails.csv")
        if 'Title' not in movie_details_df.columns or 'Factoid' not in movie_details_df.columns or 'Rotten Tomatoes Score' not in movie_details_df.columns:
            print("CSV missing required columns")
            return
    except:
        print("Error loading movieDetails.csv")
        return

    for _, movie_row in movies_df.iterrows():
        title = movie_row["title"]
        director_name = movie_row["director"]
        studio_name = movie_row["studio"]
        print(f"Processing {title}")

        try:
            response = requests.get(f"http://www.omdbapi.com/?t={requests.utils.quote(title)}&apikey={OMDBKEY}")
            if response.status_code != 200 or response.json().get("Response") == "False":
                raise Exception("API error")

            data = response.json()

            plot = data.get("Plot", "No Plot Found")

            movie_detail_row = movie_details_df.loc[movie_details_df['Title'] == title]
            if not movie_detail_row.empty:
                factoid = movie_detail_row['Factoid'].values[0].strip()
                rt_rating = get_rotten_tomatoes_rating({'Rotten Tomatoes Score': movie_detail_row['Rotten Tomatoes Score'].values[0]}, data)
            else:
                factoid = "No Factoid Found"
                rt_rating = None

            if factoid == "No Factoid Found" or rt_rating is None:
                cursor.execute("INSERT INTO ErrorMovies (title) VALUES (?)", title)
                conn.commit()
                print(f"Missing factoid or rating for {title}")
                continue

            cursor.execute("SELECT studio_id FROM Studios WHERE studio_name = ?", studio_name)
            studio_row = cursor.fetchone()
            if studio_row:
                studio_id = studio_row[0]
            else:
                cursor.execute("INSERT INTO Studios (studio_name) OUTPUT INSERTED.studio_id VALUES (?)", studio_name)
                studio_id = cursor.fetchone()[0]

            cursor.execute("SELECT director_id FROM Directors WHERE director_name = ? AND studio_id = ?", (director_name, studio_id))
            director_row = cursor.fetchone()
            if director_row:
                director_id = director_row[0]
            else:
                cursor.execute("INSERT INTO Directors (director_name, studio_id) OUTPUT INSERTED.director_id VALUES (?, ?)", (director_name, studio_id))
                director_id = cursor.fetchone()[0]

            cursor.execute("SELECT movie_id FROM Movies WHERE title = ?", title)
            existing = cursor.fetchone()
            if existing:
                cursor.execute("UPDATE Movies SET plot = ?, rating = ?, director_id = ?, factoid = ? WHERE movie_id = ?", (plot, rt_rating, director_id, factoid, existing[0]))
            else:
                cursor.execute("INSERT INTO Movies (title, plot, rating, director_id, factoid) VALUES (?, ?, ?, ?, ?)", (title, plot, rt_rating, director_id, factoid))
            conn.commit()
            print(f"Processed {title}")

        except:
            cursor.execute("INSERT INTO ErrorMovies (title) VALUES (?)", title)
            conn.commit()
            print(f"Error processing {title}")

    cursor.close()
    conn.close()
    print("Processing complete")

if __name__ == "__main__":
    main()
