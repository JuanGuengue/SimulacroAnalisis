import os
import psycopg2
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("✅ Conexión exitosa. Versión de PostgreSQL:", version[0])
    cur.execute("SELECT current_database();")
    print("base de datos: ",cur.fetchone()[0])

    cur.close()
    conn.close()
except Exception as e:
    print("❌ Error al conectar:", e)

