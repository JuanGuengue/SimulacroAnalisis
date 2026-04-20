# import csv
# import psycopg2
# import os
# import time
# from dotenv import load_dotenv

# load_dotenv(r"C:\Users\gueng\OneDrive\Escritorio\simulacro\.env")

# ruta = r"C:\Users\gueng\OneDrive\Escritorio\simulacro\Amazon Saler Report4.csv"

# conn = psycopg2.connect(
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT")
# )
# conn.set_client_encoding("LATIN1")
# cur = conn.cursor()

# # Evitar bloqueos infinitos
# cur.execute("SET lock_timeout = '5s';")
# cur.execute("SET statement_timeout = '30s';")

# # SKUs ya existentes en BD
# cur.execute("SELECT sku FROM dim_producto;")
# sku_db = set(x[0] for x in cur.fetchall())

# def limpiar_texto(valor, max_len=50):
#     v = (valor or "").strip()
#     v = v.replace("\x00", "")
#     return v[:max_len]

# sku_visto = set()
# insertados = 0
# saltados = 0
# leidas = 0
# t0 = time.time()

# # Total filas para porcentaje
# with open(ruta, newline="", encoding="latin-1", errors="replace") as f:
#     total = sum(1 for _ in f) - 1

# with open(ruta, newline="", encoding="latin-1", errors="replace") as f:
#     reader = csv.DictReader(f)

#     for fila in reader:
#         leidas += 1

#         sku = limpiar_texto(fila.get("SKU"), 50)
#         nombre = limpiar_texto(fila.get("Style"), 50) or "DESCONOCIDO"
#         categoria = limpiar_texto(fila.get("Category"), 50) or "DESCONOCIDO"
#         talla = limpiar_texto(fila.get("Size"), 50) or "DESCONOCIDO"

#         if not sku or sku in sku_visto or sku in sku_db:
#             saltados += 1
#         else:
#             try:
#                 cur.execute("""
#                     INSERT INTO dim_producto (sku, nombre, categoria, talla)
#                     VALUES (%s, %s, %s, %s)
#                 """, (sku, nombre, categoria, talla))
#                 insertados += 1
#                 sku_visto.add(sku)
#                 sku_db.add(sku)
#             except Exception as e:
#                 print("\nERROR al insertar:")
#                 print("Tipo:", type(e))
#                 print("Detalle:", repr(e))
#                 print("Fila:", fila)
#                 conn.rollback()
#                 raise

#         # Commit por tandas para reducir bloqueos
#         if leidas % 500 == 0:
#             conn.commit()

#         # Progreso en tiempo real
#         if leidas % 100 == 0:
#             elapsed = time.time() - t0
#             pct = (leidas / total) * 100 if total > 0 else 100
#             vel = leidas / elapsed if elapsed > 0 else 0
#             eta = (total - leidas) / vel if vel > 0 else 0
#             print(
#                 f"Leídas: {leidas}/{total} ({pct:.2f}%) | "
#                 f"Insertadas: {insertados} | Saltadas: {saltados} | ETA: {eta:.1f}s"
#             )

# conn.commit()

# elapsed_total = time.time() - t0
# print("\n--- FINAL ---")
# print(f"Leídas: {leidas}")
# print(f"Insertadas: {insertados}")
# print(f"Saltadas: {saltados}")
# print(f"Tiempo total: {elapsed_total:.2f}s")

# cur.close()
# conn.close()


# import csv
# import os
# import time
# import unicodedata
# import psycopg2
# from dotenv import load_dotenv

# load_dotenv(r"C:\Users\gueng\OneDrive\Escritorio\simulacro\.env")

# ruta = r"C:\Users\gueng\OneDrive\Escritorio\simulacro\Amazon Saler Report4.csv"

# def limpiar(v, max_len):
#     x = (v or "").strip()
#     x = x.replace("\x00", "")
#     x = unicodedata.normalize("NFKC", x)
#     x = x.encode("utf-8", "ignore").decode("utf-8", "ignore")
#     if not x:
#         x = "DESCONOCIDO"
#     return x[:max_len]

# conn = psycopg2.connect(
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT"),
#     options="-c client_encoding=UTF8"
# )
# cur = conn.cursor()

# cur.execute("SET lock_timeout = '5s';")
# cur.execute("SET statement_timeout = '30s';")

# # combos de envio ya existentes en BD
# cur.execute("SELECT ship_service_level, ship_state, ciudad, pais FROM dim_envio;")
# envio_db = set((a or "", b or "", c or "", d or "") for a, b, c, d in cur.fetchall())

# envio_visto = set()
# insertados = 0
# saltados = 0
# leidas = 0
# t0 = time.time()

# # total filas para progreso
# with open(ruta, newline="", encoding="utf-8", errors="replace") as f:
#     total = sum(1 for _ in f) - 1

# with open(ruta, newline="", encoding="utf-8", errors="replace") as f:
#     reader = csv.DictReader(f)

#     for fila in reader:
#         leidas += 1

#         canal = limpiar(fila.get("ship-service-level"), 50)
#         estado = limpiar(fila.get("ship-state"), 50)
#         ciudad = limpiar(fila.get("ship-city"), 100)
#         pais = limpiar(fila.get("ship-country"), 50)

#         key = (canal, estado, ciudad, pais)

#         if key in envio_visto or key in envio_db:
#             saltados += 1
#         else:
#             try:
#                 cur.execute("""
#                     INSERT INTO dim_envio (ship_service_level, ship_state, ciudad, pais)
#                     VALUES (%s, %s, %s, %s);
#                 """, (canal, estado, ciudad, pais))
#                 insertados += 1
#                 envio_visto.add(key)
#                 envio_db.add(key)
#             except Exception as e:
#                 print("\nERROR al insertar en dim_envio")
#                 print("Tipo:", type(e))
#                 print("Detalle:", repr(e))
#                 print("Fila:", fila)
#                 conn.rollback()
#                 raise

#         if leidas % 500 == 0:
#             conn.commit()

#         if leidas % 1000 == 0:
#             elapsed = time.time() - t0
#             pct = (leidas / total) * 100 if total > 0 else 100
#             vel = leidas / elapsed if elapsed > 0 else 0
#             eta = (total - leidas) / vel if vel > 0 else 0
#             print(
#                 f"Leídas: {leidas}/{total} ({pct:.2f}%) | "
#                 f"Insertadas: {insertados} | Saltadas: {saltados} | ETA: {eta:.1f}s"
#             )

# conn.commit()

# elapsed_total = time.time() - t0
# print("\n--- FINAL dim_envio ---")
# print(f"Leídas: {leidas}")
# print(f"Insertadas: {insertados}")
# print(f"Saltadas: {saltados}")
# print(f"Tiempo total: {elapsed_total:.2f}s")

# cur.close()
# conn.close()


























# total de filas del CSV para mostrar progreso
# with open(ruta, newline="", encoding="utf-8") as f:
#     total = sum(1 for _ in f) - 1  # menos encabezado

# # traemos fechas que ya existen en la tabla
# cur.execute("SELECT fecha_completa::text FROM dim_tiempo;")
# fechas_db = set(x[0] for x in cur.fetchall())

# fechas_vistas = set()   # fechas vistas dentro de este archivo
# insertadas = 0
# saltadas = 0
# leidas = 0

# t0 = time.time()

# with open(ruta, newline="", encoding="utf-8") as f:
#     reader = csv.DictReader(f)

#     for fila in reader:
#         leidas += 1
#         fecha = fila["Date"]

#         # si ya estaba en BD o ya apareció en este mismo CSV, no insertamos
#         if fecha in fechas_db or fecha in fechas_vistas:
#             saltadas += 1
#         else:
#             cur.execute("""
#                 INSERT INTO dim_tiempo (fecha_completa, dia, mes, trimestre, anio, semana_del_anio)
#                 VALUES (%s, %s, %s, %s, %s, %s);
#             """, (
#                 fila["Date"],
#                 int(fila["Day"]),
#                 int(fila["Month"]),
#                 int(fila["Quarter"]),
#                 int(fila["Year"]),
#                 int(fila["WeekYear"])
#             ))

#             insertadas += 1
#             fechas_vistas.add(fecha)
#             fechas_db.add(fecha)

#         # progreso cada 1000 filas
#         if leidas % 1000 == 0:
#             elapsed = time.time() - t0
#             pct = (leidas / total) * 100 if total > 0 else 100
#             vel = leidas / elapsed if elapsed > 0 else 0
#             eta = (total - leidas) / vel if vel > 0 else 0
#             print(
#                 f"Leídas: {leidas}/{total} ({pct:.2f}%) | "
#                 f"Insertadas: {insertadas} | Saltadas: {saltadas} | ETA: {eta:.1f}s"
#             )

# conn.commit()

# elapsed_total = time.time() - t0
# print("\n--- FINAL ---")
# print(f"Leídas: {leidas}")
# print(f"Insertadas: {insertadas}")
# print(f"Saltadas: {saltadas}")
# print(f"Tiempo total: {elapsed_total:.2f}s")

# cur.close()
# conn.close()




import csv
import os
import time
import unicodedata
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(r"C:\Users\gueng\OneDrive\Escritorio\simulacro\.env")
ruta = r"C:\Users\gueng\OneDrive\Escritorio\simulacro\Amazon Saler Report4.csv"

BATCH_SIZE = 1000

def limpiar_txt(v):
    x = (v or "").strip()
    x = x.replace("\x00", "")
    x = unicodedata.normalize("NFKC", x)
    x = x.encode("utf-8", "ignore").decode("utf-8", "ignore")
    return x

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    options="-c client_encoding=UTF8"
)
cur = conn.cursor()

cur.execute("SET lock_timeout = '5s';")
cur.execute("SET statement_timeout = '60s';")

# MAPAS DE DIMENSIONES
cur.execute("SELECT fecha_completa::text, id_tiempo FROM dim_tiempo;")
map_tiempo = {f: i for f, i in cur.fetchall()}

cur.execute("SELECT sku, id_producto FROM dim_producto;")
map_producto = {limpiar_txt(s): i for s, i in cur.fetchall()}

cur.execute("SELECT ship_service_level, ship_state, ciudad, pais, id_envio FROM dim_envio;")
map_envio = {
    (
        limpiar_txt(a) or "DESCONOCIDO",
        limpiar_txt(b) or "DESCONOCIDO",
        limpiar_txt(c) or "DESCONOCIDO",
        limpiar_txt(d) or "DESCONOCIDO",
    ): i
    for a, b, c, d, i in cur.fetchall()
}

# ORDER_ID YA EXISTENTES (evita duplicados si re-ejecutas)
cur.execute("SELECT order_id FROM fact_ventas;")
ordenes_db = set(str(x[0]) for x in cur.fetchall())

with open(ruta, newline="", encoding="utf-8", errors="replace") as f:
    total = sum(1 for _ in f) - 1

insertados = 0
saltados = 0
leidas = 0
rows_batch = []
t0 = time.time()

sql_insert = """
INSERT INTO fact_ventas
(order_id, id_tiempo, id_producto, id_envio, amount, qty, ticket_promedio)
VALUES %s
"""

with open(ruta, newline="", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)

    for fila in reader:
        leidas += 1

        order_id = limpiar_txt(fila.get("Order ID"))
        fecha = limpiar_txt(fila.get("Date"))
        sku = limpiar_txt(fila.get("SKU"))

        canal = limpiar_txt(fila.get("ship-service-level")) or "DESCONOCIDO"
        estado = limpiar_txt(fila.get("ship-state")) or "DESCONOCIDO"
        ciudad = limpiar_txt(fila.get("ship-city")) or "DESCONOCIDO"
        pais = limpiar_txt(fila.get("ship-country")) or "DESCONOCIDO"

        if not order_id or not fecha or not sku:
            saltados += 1
            continue

        if order_id in ordenes_db:
            saltados += 1
            continue

        id_tiempo = map_tiempo.get(fecha)
        id_producto = map_producto.get(sku)
        id_envio = map_envio.get((canal, estado, ciudad, pais))

        if not id_tiempo or not id_producto or not id_envio:
            saltados += 1
            continue

        try:
            qty = int(float(limpiar_txt(fila.get("Qty"))))
            amount = float(limpiar_txt(fila.get("Amount")))
            if qty <= 0:
                saltados += 1
                continue
            ticket = amount / qty
        except:
            saltados += 1
            continue

        rows_batch.append((order_id, id_tiempo, id_producto, id_envio, amount, qty, ticket))
        ordenes_db.add(order_id)

        if len(rows_batch) >= BATCH_SIZE:
            execute_values(cur, sql_insert, rows_batch, page_size=BATCH_SIZE)
            conn.commit()
            insertados += len(rows_batch)
            rows_batch = []

        if leidas % 1000 == 0:
            elapsed = time.time() - t0
            pct = (leidas / total) * 100 if total > 0 else 100
            vel = leidas / elapsed if elapsed > 0 else 0
            eta = (total - leidas) / vel if vel > 0 else 0
            print(f"Leídas: {leidas}/{total} ({pct:.2f}%) | Insertadas: {insertados} | Saltadas: {saltados} | ETA: {eta:.1f}s")

# Inserta últimas filas pendientes
if rows_batch:
    execute_values(cur, sql_insert, rows_batch, page_size=BATCH_SIZE)
    conn.commit()
    insertados += len(rows_batch)

elapsed_total = time.time() - t0
print("\n--- FINAL fact_ventas ---")
print(f"Leídas: {leidas}")
print(f"Insertadas: {insertados}")
print(f"Saltadas: {saltados}")
print(f"Tiempo total: {elapsed_total:.2f}s")

cur.close()
conn.close()
