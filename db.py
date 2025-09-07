import pyodbc

from config import DB_SERVER, DB_DATABASE

def test_db_connection():
    conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        row = cursor.fetchone()
        print("Connected to DB:", row[0])
        conn.close()
    except Exception as e:
        print("Connection failed:", e)

if __name__ == "__main__":
    test_db_connection()
    
    
