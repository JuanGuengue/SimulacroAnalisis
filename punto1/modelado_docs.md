# Documentación del Modelado OLAP

## 1. ¿Por qué esquema estrella?
Se eligió un esquema estrella porque:
- Simplifica las consultas analíticas.
- Facilita la construcción de dashboards en Power BI/Tableau.
- Permite separar métricas (hechos) de atributos descriptivos (dimensiones).

## 2. Diferencia OLTP vs OLAP
- **OLTP**: Base transaccional, optimizada para registrar operaciones (ej. compras en la tienda online).
- **OLAP**: Base analítica, optimizada para consultas agregadas y reportes ejecutivos.
RetailCo necesita ambas: OLTP para operar día a día y OLAP para analizar tendencias y tomar decisiones.

## 3. Columnas excluidas
Del dataset original se excluyeron columnas redundantes o poco útiles para análisis, como:
- Identificadores internos sin valor analítico.
- Campos de texto libre (comentarios, notas).
- Datos duplicados que ya se representan en dimensiones.
