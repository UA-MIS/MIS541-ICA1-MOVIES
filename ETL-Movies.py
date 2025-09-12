import requests
import pyodbc
import json
import pandas as pd
import time
from config import OMDBKEY, DB_SERVER, DB_DATABASE
from models import Movie


# API CALL
def fetch_movie_data(title):
    """Call OMDB API by movie title."""
    url = f"http://www.omdbapi.com/?t={title}&apikey={OMDBKEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get("Response") == "True":
            return data
        else:
            print(f"Movie not found in OMDB: {title}")
    else:
        print(f"Error {response.status_code} for {title}")
    return None


def get_rotten_tomatoes_rating(data):
    """Extract Rotten Tomatoes rating from OMDB Ratings list."""
    ratings = data.get("Ratings", [])
    for r in ratings:
        if r["Source"] == "Rotten Tomatoes":
            return r["Value"]
    return None


# DB CONNECTION 
def get_connection():
    conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)


def insert_movies(df):
    """Insert DataFrame rows into Movies, linking Directors and Studios properly."""
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        #  Handle Director
        director_id = None
        if row["Director"]:
            cursor.execute("SELECT DirectorID FROM Directors WHERE DirectorName = ?", row["Director"])
            director = cursor.fetchone()
            if director:
                director_id = director[0]
            else:
                cursor.execute(
                    "INSERT INTO Directors (DirectorName) OUTPUT INSERTED.DirectorID VALUES (?)",
                    row["Director"]
                )
                director_id = cursor.fetchone()[0]

        # Handle Studio
        studio_id = None
        if row["Studio"]:
            cursor.execute("SELECT StudioID FROM Studios WHERE StudioName = ?", row["Studio"])
            studio = cursor.fetchone()
            if studio:
                studio_id = studio[0]
            else:
                cursor.execute(
                    "INSERT INTO Studios (StudioName) OUTPUT INSERTED.StudioID VALUES (?)",
                    row["Studio"]
                )
                studio_id = cursor.fetchone()[0]

        # Insert Movie
        cursor.execute("""
            INSERT INTO Movies (Title, DirectorID, StudioID, Plot, Rating)
            VALUES (?, ?, ?, ?, ?)
        """, row["Title"], director_id, studio_id, row["Plot"], row["Rating"])

    conn.commit()
    conn.close()
    print("\n✅ Data inserted into Movies table.")


def log_error_movie(title, director, studio, reason):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ErrorMovies (Title, Director, Studio, ErrorMessage) VALUES (?, ?, ?, ?)",
        title, director, studio, reason
    )
    conn.commit()
    conn.close()


# Main
def main():
    try:
        with open("movies.json", "r") as f:
            movies = json.load(f)
    except FileNotFoundError:
        print("movies.json not found!")
        return

    # Load CSV data as a DataFrame for fallback/enrichment
    try:
        csv_df = pd.read_csv("movieDetails.csv")
    except FileNotFoundError:
        print("movieDetails.csv not found!")
        csv_df = pd.DataFrame()

    df = pd.DataFrame(columns=["Title", "Director", "Studio", "Plot", "Rating", "Factoid", "Budget", "BoxOffice", "RTScore"])

    for movie in movies:
        title = movie.get("title")
        director = movie.get("director")
        studio = movie.get("studio")

        if not title:
            continue

        data = fetch_movie_data(title)
        plot = ""
        rating = None
        factoid = None
        budget = None
        boxoffice = None
        rtscore = None

        if data:
            plot = data.get("Plot", "")
            rating = get_rotten_tomatoes_rating(data)

        # If OMDB rating is missing, try to get from CSV
        if rating is None and not csv_df.empty:
            csv_row = csv_df[csv_df["Title"].str.lower() == title.lower()]
            if not csv_row.empty:
                rtscore = csv_row.iloc[0].get("Rotten Tomatoes Score")
                rating = f"{rtscore}%" if pd.notnull(rtscore) else None
                factoid = csv_row.iloc[0].get("Factoid")
                budget = csv_row.iloc[0].get("Budget (Millions)")
                boxoffice = csv_row.iloc[0].get("Box Office Earnings (Millions)")
            else:
                log_error_movie(title, director, studio, "Null rating (possibly misspelled title)")
                continue
        elif data:
            # Enrich with CSV if available
            if not csv_df.empty:
                csv_row = csv_df[csv_df["Title"].str.lower() == title.lower()]
                if not csv_row.empty:
                    factoid = csv_row.iloc[0].get("Factoid")
                    budget = csv_row.iloc[0].get("Budget (Millions)")
                    boxoffice = csv_row.iloc[0].get("Box Office Earnings (Millions)")
                    rtscore = csv_row.iloc[0].get("Rotten Tomatoes Score")

        else:
            log_error_movie(title, director, studio, "Movie not found in OMDB")
            continue

        movie_obj = Movie(title, director, studio, plot, rating)
        df.loc[len(df)] = [
            movie_obj.title, movie_obj.director, movie_obj.studio, movie_obj.plot, movie_obj.rating,
            factoid, budget, boxoffice, rtscore
        ]
        print(f"Created Movie object: {movie_obj}")

        time.sleep(1)

    print("\nFinal DataFrame:")
    print(df)

    if not df.empty:
        insert_movies(df)


if __name__ == "__main__":
    main()
