import requests
import pyodbc
import json
import pandas as pd
import time
import re

from config import OMDBKEY, DB_SERVER, DB_DATABASE


# API Configuration
API_URL = f"http://www.omdbapi.com/?apikey={OMDBKEY}&"  

def main():
    print("Hello World")

main()