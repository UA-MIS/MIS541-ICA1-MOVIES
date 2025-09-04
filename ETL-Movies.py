import requests
import pyodbc
import json
import pandas as pd
import time
import re
from sqlalchemy import create_engine

from config import OMDBKEY, DB_SERVER, DB_DATABASE, USERNAME, PASSWORD, DB


# API Configuration
# API_URL = f"http://www.omdbapi.com/?i=tt3896198&apikey={OMDBKEY}&"  
# API_URL = f"http://www.omdbapi.com/?apikey={OMDBKEY}&"  
API_URL = f"http://www.omdbapi.com/?t= &apikey={OMDBKEY}&"  
USERNAME = {USERNAME}
PASSWORD = {PASSWORD}
DB = {DB} 

def main():
    with open("movies.json", "r") as file:
        movies_data = json.load(file)
    
    updated_movies = []
    for movie in movies_data:
        
        title = movie["title"]
        # response = requests.get(f"API_URL"t=movie["title"])
        response = requests.get(API_URL)

        if response.status_code == 200:
            data = response.json()
            api_movie_title = data.get("title")
            # if api_movie_title and api_movie_title.lower() == title.lower():
            if "Ratings" in data:
                for rating in data["Ratings"]:
                    if rating["Source"] == "Rotten Tomatoes":
                        rotten_tomatoes = rating["Value"]
                        break
            else:
                print("no rating data")

            combine = {
                # "Movie Id": data.get("imdbID"),
                "Movie Title": movie["title"],
                "Director": movie["director"],
                "Production Studio": movie["studio"],
                "Rotton Tomatoes Rating": rotten_tomatoes,
                "Plot Description": data.get("Plot")
            }
            updated_movies.append(combine)
            # print(results, "hi")
        else:
            print("bad")
            updated_movies.append(movie)

    dataFrameStuff = pd.DataFrame(updated_movies)
    sql = create_engine("mysql+mysqlconnector://{USERNAME}:{PASSWORD}@localhost:3306/{DB}")

    dataFrameStuff.to_sql(
        name="movies",
        con="engine",
        if_exists="append",
        index=False
    )

    with open("movies_updated.json", "w") as file:
        json.dump(movies_data, file, indent=4)
    
    print(dataFrameStuff)


main()
