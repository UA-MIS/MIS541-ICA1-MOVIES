


# import requests
# import pyodbc
# import json
# import pandas as pd
# import time
# import re

# from config import OMDBKEY, DB_SERVER, DB_DATABASE

# BASE_URL = "http://www.omdbapi.com/"

# def get_movie_data(title: str):
#     """Fetch movie data from OMDB API and return relevant fields."""
#     params = {"t": title, "apikey": OMDBKEY}
#     response = requests.get(BASE_URL, params=params)

#     if response.status_code != 200:
#         print(f"Error fetching {title}: {response.status_code}")
#         return None

#     data = response.json()
#     if data.get("Response") == "False":
#         print(f"Movie not found: {title}")
#         return {"Title": title, "Plot": None, "Rating": None, "Director": None, "Studio": None}

#     # Rotten Tomatoes rating
#     rotten_rating = None
#     for rating in data.get("Ratings", []):
#         if rating["Source"] == "Rotten Tomatoes":
#             rotten_rating = rating["Value"]
#             break

#     return {
#         "Title": data.get("Title", title),
#         "Plot": data.get("Plot"),
#         "Rating": rotten_rating,
#         "Director": data.get("Director") or "Unknown",
#         "Studio": data.get("Production") or "Unknown"
#     }


# def insert_into_db(df: pd.DataFrame):
#     """Insert DataFrame rows into SQL Server Movie, Director, Studio, and ErrorMovies tables."""

#     conn = pyodbc.connect(
#         f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;"
#     )
#     cursor = conn.cursor()

#     for _, row in df.iterrows():
#         title = row["Title"]
#         plot = row["Plot"]
#         rating = row["Rating"]
#         director = row["Director"]
#         studio = row["Studio"]

#         # If plot is missing → goes into ErrorMovies
#         if not plot:
#             cursor.execute("""
#                 INSERT INTO ErrorMovies (Title, Plot, Rating)
#                 VALUES (?, ?, ?)
#             """, title, plot, rating)
#             continue  # skip Movie insertion

#         # Replace missing director/studio with "Unknown"
#         director = director if director and director != "N/A" else "Unknown"
#         studio = studio if studio and studio != "N/A" else "Unknown"

#         # Insert director if not exists
#         cursor.execute("SELECT DirectorID FROM Director WHERE Name = ?", director)
#         director_row = cursor.fetchone()
#         if director_row:
#             director_id = director_row[0]
#         else:
#             cursor.execute("INSERT INTO Director (Name) VALUES (?)", director)
#             cursor.commit()  # commit to generate ID
#             cursor.execute("SELECT DirectorID FROM Director WHERE Name = ?", director)
#             director_id = cursor.fetchone()[0]

#         # Insert studio if not exists
#         cursor.execute("SELECT StudioID FROM Studio WHERE Name = ?", studio)
#         studio_row = cursor.fetchone()
#         if studio_row:
#             studio_id = studio_row[0]
#         else:
#             cursor.execute("INSERT INTO Studio (Name) VALUES (?)", studio)
#             cursor.commit()  # commit to generate ID
#             cursor.execute("SELECT StudioID FROM Studio WHERE Name = ?", studio)
#             studio_id = cursor.fetchone()[0]

#         # Insert movie
#         cursor.execute(
#             """
#             INSERT INTO Movie (Title, Plot, Rating, Factoid, DirectorID, StudioID)
#             VALUES (?, ?, ?, ?, ?, ?)
#             """,
#             title, plot, rating, None, director_id, studio_id
#         )

#     conn.commit()
#     cursor.close()
#     conn.close()
#     print(" Data inserted into database successfully.")



# def main():
#     with open("movies.json", "r") as f:
#         movies = json.load(f)

#     results = []
#     for movie in movies:
#         movie_data = get_movie_data(movie["title"])
#         if movie_data:
#             results.append(movie_data)
#         time.sleep(0.5)  # avoid API rate limit

#     df = pd.DataFrame(results, columns=["Title", "Plot", "Rating", "Director", "Studio"])
#     print(df)
#     insert_into_db(df)

# if __name__ == "__main__":
#     main()











# map the columns of the dataframe to the database. make an ErrorMovies table in the db and put all the mispelled movies in there. 
# ErrorMovies table does not have to be related to the other tables in the DB.

# IMPORTANT: Map all movies into the same dataframe regardless of whether or not they have errors. 
# Once the movies are in the dataframe, check if they have anything in the "plot" data field. 
# The mispelled movies will not have anything in the plot column. 
# These movies should be thrown in the ErrorMovies table and the correctly spelled movies should be put in the Movies table.






import requests
import pyodbc
import json
import pandas as pd
import time
import re          

from config import OMDBKEY, DB_SERVER, DB_DATABASE

BASE_URL = "http://www.omdbapi.com/"

def get_movie_data(title: str):
    """Fetch movie data from OMDB API and return Title, Plot, Director, Studio.
       Returns Plot=None etc. when not found."""
    # if the movie isnt found it jusr retruns none
    params = {"t": title, "apikey": OMDBKEY}
    #this tells omdb which movie to look up



    # if connection fails, it returns none
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
    except Exception as e:
        print(f"Error fetching {title}: {e}")
        return {"Title": title, "Plot": None, "Director": None, "Studio": None}

    #checks to see if omdb is responding correctly
    if response.status_code != 200:
        print(f"Error fetching {title}: {response.status_code}")
        return {"Title": title, "Plot": None, "Director": None, "Studio": None}


    data = response.json()
    if data.get("Response") == "False":
        print(f"Movie not found: {title}")
        return {"Title": title, "Plot": None, "Director": None, "Studio": None}

    
    
    
    
    
    #this returns the movie info
    return {
        "Title": data.get("Title", title),
        "Plot": data.get("Plot"),
        "Director": data.get("Director") or None,
        "Studio": data.get("Production") or None
    }

    
    
    
    #this puts the movies into the panda dataframe. still not 100 percent sure whats actuallt going on behind the scenes here
def insert_into_db(df: pd.DataFrame):
    """Insert rows into Movie and ErrorMovies, creating Director/Studio as needed."""

    #not sure what this is but chat said i needed it
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;"
    )
    cursor = conn.cursor()



    #keeping count of everything
    inserted_movies = 0
    inserted_errors = 0



    #looping through the table. pulling info from it and cleaning it up
    for idx, row in df.iterrows():
        title = row["Title"]
        plot = row["Plot"] if not pd.isna(row["Plot"]) else None
        # CSV 'Rating' (numeric or string) -> convert to string or None
        raw_rating = row.get("Rating", None)
        rating = None if pd.isna(raw_rating) else str(raw_rating).strip()
        factoid_raw = row.get("Factoid", None)
        factoid = None if pd.isna(factoid_raw) else str(factoid_raw).strip()

        director = row.get("Director", None)
        studio = row.get("Studio", None)

        # If theres not plot, it goes to the ErrorMovies table
        if not plot:
            try:
                cursor.execute(
                    "INSERT INTO ErrorMovies (Title, Plot, Rating) VALUES (?, ?, ?)",
                    title, plot, rating
                )
                conn.commit()
                inserted_errors += 1
            except Exception as e:
                print(f"[ERROR] inserting into ErrorMovies for '{title}': {e}")
            continue

        # if the director and studio arent there it uses Uknown
        director = director if director and not pd.isna(director) and director != "N/A" else "Unknown"
        studio = studio if studio and not pd.isna(studio) and studio != "N/A" else "Unknown"

        # Get or create DirectorID
        try:
            cursor.execute("SELECT DirectorID FROM Director WHERE Name = ?", director)
            drow = cursor.fetchone()
            if drow:
                director_id = drow[0]
            else:
                cursor.execute("INSERT INTO Director (Name) VALUES (?)", director)
                conn.commit()
                cursor.execute("SELECT DirectorID FROM Director WHERE Name = ?", director)
                director_id = cursor.fetchone()[0]
        except Exception as e:
            print(f"[ERROR] Director handling for '{title}' (director='{director}'): {e}")
            continue

        # Get or create StudioID
        try:
            cursor.execute("SELECT StudioID FROM Studio WHERE Name = ?", studio)
            srow = cursor.fetchone()
            if srow:
                studio_id = srow[0]
            else:
                cursor.execute("INSERT INTO Studio (Name) VALUES (?)", studio)
                conn.commit()
                cursor.execute("SELECT StudioID FROM Studio WHERE Name = ?", studio)
                studio_id = cursor.fetchone()[0]
        except Exception as e:
            print(f"[ERROR] Studio handling for '{title}' (studio='{studio}'): {e}")
            continue

        # Put the movies into the Movie table
        try:
            cursor.execute(
                "INSERT INTO Movie (Title, Plot, Rating, Factoid, DirectorID, StudioID) VALUES (?, ?, ?, ?, ?, ?)",
                title, plot, rating, factoid, director_id, studio_id
            )
            conn.commit()
            inserted_movies += 1
        except Exception as e:
            print(f"[ERROR] inserting Movie '{title}': {e}")
            # don't raise — continue to try other rows
            continue


    #closes the connection and says if it worked
    cursor.close()
    conn.close()
    print(f"Finished: inserted {inserted_movies} movies, {inserted_errors} error rows.")

def main():
    # Load movies.json
    with open("movies.json", "r", encoding="utf-8") as f:
        movies = json.load(f)

    results = []
    for m in movies:
        title = m.get("title")
        movie_data = get_movie_data(title)
        results.append(movie_data)
        time.sleep(0.5)  # polite delay

    # OMDB DF
    omdb_df = pd.DataFrame(results, columns=["Title", "Plot", "Director", "Studio"])

    # Read CSV chat gpt gave me this
    try:
        csv_df = pd.read_csv("movieDetails.csv", sep=None, engine="python")
    except Exception:
        csv_df = pd.read_csv("movieDetails.csv")

    # Ensure expected CSV columns exist
    expected_csv_cols = {"Title", "Rating", "Factoid"}
    missing = expected_csv_cols - set(csv_df.columns)
    if missing:
        raise SystemExit(f"CSV missing columns: {missing}. Expected columns: {expected_csv_cols}. "
                         "Check movieDetails.csv header exactly.")

    # Keep only CSV columns we need to merge
    csv_df = csv_df[["Title", "Rating", "Factoid"]]

    # Chat gpt somehow merges this. not too sure whats going on here but it works. merges from omdb and csv file
    merged_df = pd.merge(omdb_df, csv_df, on="Title", how="left")

    # Keep and order exactly the columns we'll use
    merged_df = merged_df[["Title", "Plot", "Rating", "Factoid", "Director", "Studio"]]

    print(merged_df.head(10))  # quick check before DB insert
    insert_into_db(merged_df)

#
if __name__ == "__main__":
    main()
