"""Ingesta batch: convierte los reportes DATASS (.xlsx) a CSV y los sube a HDFS.

Spark 3.2 no lee .xlsx de forma nativa, asi que este paso (ejecutado en el host,
requiere pandas + openpyxl) deja una copia CSV en raw/infraestructura/ junto al
.xlsx original. No se modifica ningun valor: solo se quita la cabecera decorativa
del reporte (filas 0-7) y se renombran las columnas a MAYUSCULAS_SIN_ESPACIOS.

Uso:  python src/ingesta_infraestructura.py
"""
import os
import subprocess

import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA = os.path.join(BASE, "data", "raw", "infraestructura")
COLUMNAS = {
    "Ubigeo": "UBIGEO", "Departamento": "DEPARTAMENTO", "Provincia": "PROVINCIA",
    "Distrito": "DISTRITO", "CentroPoblado": "CENTRO_POBLADO", "Observado": "OBSERVADO",
    "Fecha de Modificacion": "FECHA_MODIFICACION", "Encuesta ID": "ENCUESTA_ID",
    "Encuesta Modulo ID": "ENCUESTA_MODULO_ID", "Nombre": "NOMBRE_SISTEMA",
    "Total de Poblacion": "POBLACION", "Total de Viviendas": "VIVIENDAS",
    "Viviendas Habitadas": "VIVIENDAS_HABITADAS", "Valor": "VALOR",
}

for nombre in ["datass_antiguedad", "datass_estado"]:
    xlsx = os.path.join(CARPETA, nombre + ".xlsx")
    df = pd.read_excel(xlsx, header=8, dtype=str)
    df = df[list(COLUMNAS)].rename(columns=COLUMNAS).dropna(how="all")
    csv = os.path.join(CARPETA, nombre + ".csv")
    df.to_csv(csv, sep=";", index=False, encoding="utf-8")
    print(f"{nombre}.xlsx -> {nombre}.csv | filas={len(df)} columnas={df.shape[1]}")

# Subida a HDFS (el namenode monta el proyecto en /archivos_locales)
cmd = ("hdfs dfs -put -f /archivos_locales/data/raw/infraestructura/datass_antiguedad.csv "
       "/archivos_locales/data/raw/infraestructura/datass_estado.csv /datalake_sedalib/raw/infraestructura/")
subprocess.run(["docker", "exec", "namenode", "bash", "-c", cmd], check=True)
print("CSV subidos a hdfs:///datalake_sedalib/raw/infraestructura/")
