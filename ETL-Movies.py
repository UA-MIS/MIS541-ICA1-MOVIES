import requests
import pyodbc
import json
import pandas as pd
import time
import re
import csv
import mysql.connector

from config import OMDBKEY, DB_SERVER, DB_DATABASE, USERNAME, PASSWORD, DB


# API Configuration
# API_URL = f"http://www.omdbapi.com/?t=Blade Runner&apikey={OMDBKEY}&"  
API_URL = "http://www.omdbapi.com/"
API_KEY = f"{OMDBKEY}"
USERNAME = f"{USERNAME}"
PASSWORD = f"{PASSWORD}"
DB = f"{DB}"

#HW: make a try catch, log the errors to a separate file or separate DB, also add factoid from csv file 
#HW pt.2: save this to mySQL DB

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
                    rt_rating = rating.get("Value")
                    rotten_tomatoes = rt_rating.strip('%')
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
    
    try:
        connector = mysql.connector.connect(
            host="localhost",
            user=USERNAME,
            password=PASSWORD,
            database=DB
        )
        print("Connection successful!")
        cursor = connector.cursor()
        movies_to_put_in_DB = [
            (
                m["title"],
                m.get("Plot Description"),
                m.get("Rotton Tomatoes Rating"),
                m.get("Factoid")
            )
            for m in updated_movies
        ]
        sql = "INSERT INTO Movies (title, plot, rating, factoid) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, movies_to_put_in_DB)

        connector.commit()
        print(f"{cursor.rowcount} movies inserted successfully!")

    except mysql.connector.Error as e:
        print("MySql Error: ", e)
    finally: 
        if 'cursor' in locals():
            cursor.close()
        if 'connector' in locals() and connector.is_connected():
            connector.close()

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
