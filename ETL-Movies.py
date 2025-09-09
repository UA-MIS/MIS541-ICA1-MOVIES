import requests
import pyodbc
import json
import pandas as pd
import time
import re
import csv
from sqlalchemy import create_engine

from config import OMDBKEY, DB_SERVER, DB_DATABASE, USERNAME, PASSWORD, DB


# API Configuration
# API_URL = f"http://www.omdbapi.com/?t=Blade Runner&apikey={OMDBKEY}&"  
API_URL = "http://www.omdbapi.com/"
API_KEY = {OMDBKEY} 
USERNAME = {USERNAME}
PASSWORD = {PASSWORD}
DB = {DB} 

#HW: make a try catch, log the errors to a separate file or separate DB, also add factoid from csv file 
def main():
    with open("movies.json", "r") as file:
        movies_data = json.load(file)

    factoids = load_factoids("movieDetails.csv")    

    updated_movies = []
    unfound_movies = []
    
    for movie in movies_data:
        title = movie["title"]
        if not title: 
            continue
        
        api_data = fetch_movies(title)
        if api_data: 
            rotten_tomatoes = None
            for rating in api_data.get("Ratings", []):
                if rating.get("Source") == "Rotten Tomatoes":
                    rotten_tomatoes = rating.get("Value")
                    break

            movie.update({
                "Rotton Tomatoes Rating": rotten_tomatoes,
                "Plot Description": api_data.get("Plot")
            })
            factoid = factoids.get(title.lower())
            if factoid: 
                movie["Factoid"] = factoid
            updated_movies.append(movie)
        else:
            unfound_movies.append(movie)

    with open("movies_updated.json", "w") as file:
        json.dump(updated_movies, file, indent=4, ensure_ascii=False)

    if unfound_movies:
        with open("movies_not_found.json", "w") as file:
            json.dump(unfound_movies, file, indent=4, ensure_ascii=False)
    
def fetch_movies(title):
    params = {
        "t": title,
        "apikey": API_KEY,
    }
    try: 
        response = requests.get(API_URL, params=params)
        data = response.json()
        if data.get("Response") == "True":
            return data
    except requests.RequestException as e:
        print(f"No data found for movie: {title}: {e}")
        return None
    

def load_factoids(csv_file):
    factoids = {}
    with open(csv_file, newline="", encoding="utf-8") as file: 
        reader = csv.DictReader(file)
        for row in reader:
            title = row.get("Title")
            factoid = row.get("Factoid")
            if title and factoid: 
                factoids[title.strip().lower()] = factoid.strip()
    return factoids

main()
