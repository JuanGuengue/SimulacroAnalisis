import pandas as pd

#cargar datos

df = pd.read_csv("Amazon Sale Report.csv")

#shape
print("Shape:", df.shape)
#types
print("\nTipos de datos:\n",df.dtypes)
#porcentaje nulos
#isnull() busca si hay null, si lo encuentra devuelvo un true, 
#mean() saca un promedio de los True y lo multiplicamos por 100 para volver un porcentaje
print("\nPorcentaje de nulos:\n",df.isnull().mean()*100)



#se identifica que la columna amount hay varios datos null
df_nulos_amount = df.loc[df["Amount"].isnull(), "Amount"]
print("nulos en la columna amount: ", len(df_nulos_amount))

#se idientifica que en la columna Qty hay cantidad 0 y ese dato nos nos sirve para cuantificar
fuera_rango = df[df["Qty"]<= 0]
print("datos menores o igual a 0 en Qty:", len(fuera_rango))

#se encuentras datos duplicados en la columna de Order ID y deben ser datos unicos
df_duplicados_OrderID = df["Order ID"].duplicated().sum()
print("duplicas en Order Id: ",df_duplicados_OrderID)


#estadisticas descripttivas, de solo columnas numericas
print(df[["Qty","Amount"]].describe())
