import requests
import pyodbc
import json
import pandas as pd
import time
import re

from config import OMDBKEY, DB_SERVER, DB_DATABASE


# API Configuration
OMDB_BASE_URL = "http://www.omdbapi.com/"
API_URL = f"http://www.omdbapi.com/?t=Blade Runner&apikey={OMDBKEY}"

# function that gets titles from omdb
def fetch_omdb_by_title(title):
    params = {"t": title, "apikey": OMDBKEY}
    r = requests.get(OMDB_BASE_URL, params=params)
    data = r.json()
    return data if data.get("Response") == "True" else None


# function that pulls the rotten tomatoes rating from omdb
def get_rotten_tomatoes(ratings_list):
    """Pull Rotten Tomatoes rating out of Ratings list."""
    if not ratings_list:
        return None
    for rating in ratings_list:
        if rating.get("Source") == "Rotten Tomatoes":
            return rating.get("Value")
    return None

def main():
# load the json list
    with open("movies.json", "r", encoding="utf-8") as f:
        movies = json.load(f)

    results = []

    # loop through json and get the data using the 2 functions
    for movie in movies:
        title = movie["title"]
        data = fetch_omdb_by_title(title)

        if data:
            plot = data.get("Plot")
            rotten = get_rotten_tomatoes(data.get("Ratings"))
        else:
            plot = None
            rotten = None

        results.append({
            "Title": title,
            "Director": movie["director"],
            "Studio": movie["studio"],
            "Plot": plot,
            "RottenTomatoes": rotten
        })

        # apparently i need to be nice to the free api
        time.sleep(0.2)

    # panda dataframe - df.columns to see columns
    # pd.set_option("display.max_rows", none) to see entire DF
    df = pd.DataFrame(results)
    print(df)

if __name__ == "__main__":
    main()

# write a loop that takes the json movies and gets back all of them from the api - use pandas data frame