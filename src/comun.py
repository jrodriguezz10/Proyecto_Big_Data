"""Utilidades compartidas por los scripts Spark del datalake SEDALIB.

- Rutas HDFS (raw / processed)
- Esquemas explicitos de TODAS las fuentes (no se usa inferSchema)
- Normalizacion de nombres geograficos (trim, upper, sin tildes, '_' -> ' ')
- Lectores de cada fuente
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               IntegerType)

HDFS = "hdfs://namenode:8020"
RAW = HDFS + "/datalake_sedalib/raw/"
PROCESSED = HDFS + "/datalake_sedalib/processed/"
PANEL = PROCESSED + "master_panel.parquet"


def S(nombre):
    return StructField(nombre, StringType(), True)


def D(nombre):
    return StructField(nombre, DoubleType(), True)


def I(nombre):
    return StructField(nombre, IntegerType(), True)


ESQ_INTERRUPCIONES = StructType([
    S("IDINTERRUPCION"), S("EPS"), S("SEDE"), S("TIPOINTERRUPCION"), S("TIPOSERVICIO"),
    S("MOTIVOINTERRUPCION"), S("FECHAINICIO"), S("HORAINICIO"), S("FECHAFIN"), S("HORAFIN"),
    S("DEPARTAMENTO"), S("PROVINCIA"), S("DISTRITO"), I("NUMCONEXDOM"), I("UNIDADESUSO"),
    I("NUMCAMIONESPUNTOS")])

ESQ_CLIMA = StructType([
    I("ID"), S("ESTACION"), S("FECHA"), S("HORA"), D("LONGITUD"), D("LATITUD"), D("ALTITUD"),
    D("TEMP"), D("HR"), D("PP"), S("RED"), S("DEPARTAMENTO"), S("PROVINCIA"), S("DISTRITO"),
    D("UBIGEO"), S("FECHA_CORTE")])

# INGEMMET trae 189 columnas; el esquema declara solo las 18 primeras (las que se
# usan). Con un esquema mas corto, Spark descarta las columnas sobrantes.
ESQ_PELIGROS = StructType([
    S("COD_PROYECTO"), S("COD_INV"), S("COD_INV_OLD"), S("ESTE"), S("NORTE"), S("ZONA"),
    S("ALTURA"), S("COD_HOJA"), S("NM_SECTOR"), S("NM_DPTO"), S("NM_PROV"), S("NM_DIST"),
    S("NOM_CUENCA"), S("FECHA"), S("EFECTUADO_POR"), S("COD_PELIGRO"), S("TIP_PELIGRO"),
    S("NOM_PELIGRO")])

ESQ_ANF = StructType([S("EPS"), S("GRUPO_EPS"), S("REGION"), D("AGUA_NO_FACTURADA_PORCENTAJE")])

ESQ_HIDROJETS = StructType([
    S("FECHA_CORTE"), I("ANIO"), S("EPS"), S("DEPARTAMENTO"), S("PROVINCIA"), S("DISTRITO"),
    S("FECHA_REGISTRO"), S("COORDENADA_ESTE"), S("COORDENADA_NORTE"), S("TIPO_SERVICIO"),
    S("TIPO_REGISTRO"), S("RED_ATENDIDA"), S("DIRECCION_INICIAL"), S("DIRECCION_FINAL"),
    I("POBLACION")])

ESQ_INFRA = StructType([
    S("UBIGEO"), S("DEPARTAMENTO"), S("PROVINCIA"), S("DISTRITO"), S("CENTRO_POBLADO"),
    S("OBSERVADO"), S("FECHA_MODIFICACION"), S("ENCUESTA_ID"), S("ENCUESTA_MODULO_ID"),
    S("NOMBRE_SISTEMA"), I("POBLACION"), I("VIVIENDAS"), I("VIVIENDAS_HABITADAS"), S("VALOR")])


def spark_session(nombre, **conf):
    b = (SparkSession.builder.appName(nombre).master("local[*]")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.session.timeZone", "UTC"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def norm(c):
    """Normaliza un nombre geografico: trim, MAYUSCULAS, sin tildes, '_' -> ' '.
    El caracter de reemplazo U+FFFD (Ñ mal codificada en origen) se mapea a 'N'."""
    x = F.upper(F.trim(c))
    x = F.translate(x, "ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ�_", "AEIOUAEIOUAEIOUNN ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def leer_interrupciones(spark):
    return spark.read.csv(RAW + "operaciones/sunass_interrupciones.csv", header=True,
                          schema=ESQ_INTERRUPCIONES, encoding="UTF-8")


def leer_clima(spark):
    return spark.read.csv(RAW + "clima/senamhi_clima.csv", header=True, schema=ESQ_CLIMA)


def leer_peligros(spark):
    return spark.read.csv(RAW + "geologia/ingemmet_peligros.csv", header=True, sep=";",
                          schema=ESQ_PELIGROS, encoding="ISO-8859-1", multiLine=True)


def leer_anf(spark):
    return spark.read.csv(RAW + "operaciones/sunass_anf.csv", header=True, schema=ESQ_ANF)


def leer_hidrojets(spark):
    return spark.read.csv(RAW + "operaciones/sunass_hidrojets.csv", header=True, sep=";",
                          schema=ESQ_HIDROJETS, encoding="ISO-8859-1")


def leer_infra(spark, nombre):
    """nombre: 'datass_antiguedad' o 'datass_estado' (CSV generado por ingesta_infraestructura.py)."""
    return spark.read.csv(RAW + "infraestructura/%s.csv" % nombre, header=True, sep=";",
                          schema=ESQ_INFRA, encoding="UTF-8")


def titulo(texto):
    print("\n" + "=" * 78)
    print(" " + texto)
    print("=" * 78)
