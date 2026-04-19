import csv
from collections import defaultdict

ventas_por_SKU = defaultdict(float)

datos = []

with open('./Amazon sale Report.csv',newline="",encoding="utf-8") as file:
    reader = csv.reader(file)
## desde aqui
##  encabzados y primeros 10
    encabezados = next(reader)
 ##   print("encabezados:", encabezados)


##    for i, fila in enumerate(reader):
##        if i<10:
##            print(fila)
##        else:
##            break
## hasta aqui


##desde aqui
##lista de diccionarios 
    for fila in reader:
        datos.append(fila)

##hasta aqui
pos_order = encabezados.index("Order ID")
pos_amount = encabezados.index("Amount")
pos_product = encabezados.index("SKU")
pos_cantidad = encabezados.index("Qty")
##desde aqui
## imprimir y sumar los amount
dato_mal = 0
dato_bien = 0
ventas = 0
for i in datos:
    try:
        numero = float(i[pos_amount])
        dato_bien = dato_bien + 1
        ventas = ventas + numero   
    except ValueError:
        dato_mal = dato_mal + 1


for i in datos:
    try:
        producto = i[pos_product]
        cantidad = int(i[pos_cantidad])
        ventas_por_SKU[producto] = ventas_por_SKU[producto] + cantidad
    except ValueError:
        pass

top_5_productos = sorted(ventas_por_SKU.items(), key=lambda x: x[1], reverse=True)[:5]
print("Ventas totales: ", round(ventas,2))
print ("datos mal:", dato_mal)
print("Top 5 productos:")
for i in top_5_productos:
    print(f"Producto: {i[0]}, Cantidades vendidas: {i[1]}")


nuevo_csv = []


for i in datos:
    registro = {
        "Order ID": i[pos_order],
        "SKU": i[pos_product],
        "Amount": i[pos_amount],
        "Qty": i[pos_cantidad]
    }
    nuevo_csv.append(registro)

with open('nuevo_reporte.csv', 'w', newline='', encoding='utf-8') as file:
    escritor = csv.DictWriter(file, fieldnames=["Order ID", "SKU", "Amount", "Qty"])
    escritor.writeheader()
    escritor.writerows(nuevo_csv)
    

