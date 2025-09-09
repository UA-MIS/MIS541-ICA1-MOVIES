import requests
import pyodbc
import json
import pandas as pd
import time

from config import OMDBKEY, DB_SERVER, DB_DATABASE

# !! API
OMDB_BASE_URL = "http://www.omdbapi.com/"

def fetch_omdb_by_title(title: str):
    params = {"t": title, "apikey": OMDBKEY}
    r = requests.get(OMDB_BASE_URL, params=params, timeout=10)
    d = r.json()
    return d if d.get("Response") == "True" else None

def get_rotten_tomatoes(ratings_list):
    """Return Rotten Tomatoes rating as INT 0..100 (or None)."""
    if not ratings_list:
        return None
    for r in ratings_list:
        if r.get("Source") == "Rotten Tomatoes":
            v = r.get("Value")  # e.g., "87%"
            if isinstance(v, str) and v.endswith("%"):
                try:
                    return int(v[:-1])
                except ValueError:
                    return None
    return None

# !! Cleaners
def first_person(name: str | None):
    """If OMDb lists multiple directors, keep the first; normalize 'N/A' -> None."""
    if not name or name == "N/A":
        return None
    return name.split(",")[0].strip()

def normalize_org(name: str | None):
    """Trim/collapse whitespace; normalize 'N/A' -> None."""
    if not name or name == "N/A":
        return None
    return " ".join(name.split())

# !! Factoid loader (Excel -> dict[normalized_title] = factoid) !!
# !! Factoid loader (Excel -> dict[normalized_title_lower] = factoid) !!
def load_factoid_map(excel_path: str = "movieDetails.xlsx", sheet_name=0):
    """
    Reads Excel and returns {lowercased_title: factoid}.
    Accepts column headers (any case): Title/Film/Movie and Factoid/Factoids.
    Forces first sheet by default for older Pandas behavior, but
    will also handle dict-of-sheets if returned.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # If Pandas gave us a dict (rare with sheet_name=0, but safe-guard anyway)
    if isinstance(df, dict):
        # take the first sheet with columns
        for _name, _df in df.items():
            if hasattr(_df, "columns"):
                df = _df
                break

    cols = {str(c).strip().lower(): c for c in df.columns}

    title_col = next((cols[c] for c in ("title", "film", "movie") if c in cols), None)
    fact_col  = next((cols[c] for c in ("factoid", "factoids") if c in cols), None)
    if not title_col or not fact_col:
        raise ValueError("Excel must have Title (or Film/Movie) and Factoid column headers.")

    df = df[[title_col, fact_col]].dropna(subset=[title_col])

    return {str(t).strip().lower(): f for t, f in zip(df[title_col], df[fact_col]) if pd.notna(f)}

# !! DB Connector !!
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={DB_SERVER};DATABASE={DB_DATABASE};"
        "Trusted_Connection=Yes;TrustServerCertificate=Yes;"
    )

# !! functions that 
def get_or_create_director(cur, name: str | None):
    if not name:
        return None
    cur.execute("SELECT DirectorID FROM Director WHERE Name = ?", (name,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute("INSERT INTO Director(Name) OUTPUT INSERTED.DirectorID VALUES (?)", (name,))
    return int(cur.fetchone()[0])

def get_or_create_studio(cur, name: str | None):
    if not name:
        return None
    cur.execute("SELECT StudioID FROM Studio WHERE Name = ?", (name,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute("INSERT INTO Studio(Name) OUTPUT INSERTED.StudioID VALUES (?)", (name,))
    return int(cur.fetchone()[0])

# !! MAIN !!
def main():
    # Load JSON list
    with open("movies.json", "r", encoding="utf-8") as f:
        movies = json.load(f)

    # !! Load Excel factoids once
    factoid_map = load_factoid_map(excel_path="movieDetails.xlsx", sheet_name=None)

    results = []

    # Loop JSON → OMDb → collect results
    for m in movies:
        title = m["title"]

        data = fetch_omdb_by_title(title)
        if data:
            plot = None if data.get("Plot") in (None, "N/A") else data.get("Plot")
            rt   = get_rotten_tomatoes(data.get("Ratings"))  # int or None

            # Prefer OMDb for Director/Studio; fallback to JSON
            director_clean = first_person(data.get("Director")) or first_person(m.get("director"))
            studio_clean   = normalize_org(data.get("Production")) or normalize_org(m.get("studio"))
        else:
            plot = None
            rt   = None
            director_clean = first_person(m.get("director"))
            studio_clean   = normalize_org(m.get("studio"))

        # !! include Factoid from Excel (match by lowercased title)
        factoid = factoid_map.get(str(title).strip().lower())

        results.append({
            "Title": title,
            "Director": director_clean,
            "Studio": studio_clean,
            "Plot": plot,
            "RottenTomatoes": rt,
            "Factoid": factoid,  # <- new
        })

        time.sleep(0.2)  # polite to OMDb

    # !! printing dataframe out to see it for debugging
    df = pd.DataFrame(results)
    print(df)

    # * Success case: rating is required, plot is optional
    success_mask = df["RottenTomatoes"].notna()
    df_success = df[success_mask].copy()
    df_errors  = df[~success_mask][["Title", "Director", "Studio"]].copy()

    # * load to sql
    conn = get_connection()
    try:
        cur = conn.cursor()

        # * caches to avoid repeated selects
        dir_cache, studio_cache = {}, {}

        # * looking up dirctor and studio name from omdb because json is wrong
        def lookup_dir(name):
            if name in dir_cache:
                return dir_cache[name]
            did = get_or_create_director(cur, name)
            dir_cache[name] = did
            return did

        def lookup_studio(name):
            if name in studio_cache:
                return studio_cache[name]
            sid = get_or_create_studio(cur, name)
            studio_cache[name] = sid
            return sid

        # * insert successes only
        for _, row in df_success.iterrows():
            did = lookup_dir(row["Director"])
            sid = lookup_studio(row["Studio"])
            cur.execute(
                "INSERT INTO Movies (Title, Plot, RottenTomatoes, Factoid, DirectorID, StudioID) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (row["Title"], row["Plot"], int(row["RottenTomatoes"]), row["Factoid"], did, sid)  # <- row["Factoid"]
            )

        # * insert errors
        if not df_errors.empty:
            cur.fast_executemany = True
            cur.executemany(
                "INSERT INTO ErrorMovies (Title, Director, Studio) VALUES (?, ?, ?)",
                list(df_errors.itertuples(index=False, name=None))
            )

        conn.commit()
        print(f"\nInserted {len(df_success)} movies and logged {len(df_errors)} errors.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
