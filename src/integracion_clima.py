"""2b. Integracion de clima (SENAMHI + NASA POWER) al panel distrito-mes como ANOMALIA relativa.

Ejecutar (despues de procesamiento_spark.py y de subir clima_nasa_la_libertad.csv a raw/clima/):
    docker exec spark_node sh -c '/spark/bin/spark-submit /app/src/integracion_clima.py 2>&1 | grep -v " INFO " | tee /app/evidencias/02b_integracion_clima.txt'

Entrada : processed/master_panel.parquet, processed/clima_distrito_mes.parquet (SENAMHI),
          raw/clima/clima_nasa_la_libertad.csv (src/ingesta_clima_nasa.py)
Salida  : processed/master_panel_clima.parquet (no se sobrescribe master_panel.parquet)

Decisiones:
* No se usan los mm absolutos (NASA es reanalisis y su nivel no es comparable con estaciones).
  PP_ANOMALIA = PP_TOTAL_MM / promedio de PP del MISMO distrito y MES calendario.
* Promedio de referencia (climatologia): 2015-01 a 2021-12, igual para ambas fuentes. Termina
  antes del primer corte de validacion (2022-01), asi ningun dato de validacion/holdout entra
  en el denominador.
* MES_LLUVIOSO = 1 si PP_ANOMALIA > 1.5.
* LA LIBERTAD: NASA POWER por centroide de distrito, JOIN por (DEPTO_N, DIST_N, ANIO, MES).
  Resto del pais: SENAMHI donde exista (LIMA, AREQUIPA, TACNA), serie departamento-mes como en
  procesamiento_spark.py (todos los distritos del departamento comparten serie y climatologia).
* Si la climatologia del mes es 0 mm (mes siempre seco) la division no existe: PP_ANOMALIA queda
  nula y MES_LLUVIOSO = 1 solo si llovio (PP > 0). Se cuenta cuantas veces ocurre.
* Se agregan los rezagos PP_ANOMALIA_LAG1 y MES_LLUVIOSO_LAG1 (mes anterior, calculado sobre la
  serie de clima completa, asi 2019-06 tiene rezago). El modelo solo usa los rezagos.
"""
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)

import comun as C
from comun import titulo

PANEL_CLIMA = C.PROCESSED + "master_panel_clima.parquet"
REF_INI, REF_FIN = 2015 * 12 + 0, 2021 * 12 + 11          # T = ANIO*12 + MES-1
UMBRAL_LLUVIOSO = 1.5
LL = "LA LIBERTAD"

ESQ_NASA = StructType([
    StructField("DEPTO_N", StringType()), StructField("PROV_N", StringType()),
    StructField("DIST_N", StringType()), StructField("UBIGEO", StringType()),
    StructField("LAT_CENTROIDE", DoubleType()), StructField("LON_CENTROIDE", DoubleType()),
    StructField("ANIO", IntegerType()), StructField("MES", IntegerType()),
    StructField("DIAS_MES", IntegerType()), StructField("PRECTOTCORR_MM_DIA", DoubleType()),
    StructField("PP_TOTAL_MM", DoubleType()), StructField("T2M", DoubleType()),
    StructField("RH2M", DoubleType()), StructField("FUENTE", StringType())])

spark = C.spark_session("SEDALIB_IntegracionClima")
T = (F.col("ANIO") * 12 + F.col("MES") - 1)


def anomalia(serie, claves, fuente):
    """serie: claves + ANIO, MES, PP. Devuelve claves + ANIO, MES, PP_ANOMALIA, MES_LLUVIOSO y rezagos."""
    s = serie.withColumn("T", T)
    clim = (s.filter((F.col("T") >= REF_INI) & (F.col("T") <= REF_FIN))
            .groupBy(*claves, "MES").agg(F.avg("PP").alias("PP_CLIMA"), F.count("*").alias("N_ANIOS_REF")))
    s = s.join(clim, claves + ["MES"], "left")
    s = (s.withColumn("PP_ANOMALIA", F.when(F.col("PP_CLIMA") > 0, F.col("PP") / F.col("PP_CLIMA")))
         .withColumn("MES_LLUVIOSO", F.when(F.col("PP_CLIMA") > 0, (F.col("PP_ANOMALIA") > UMBRAL_LLUVIOSO).cast("int"))
                     .when(F.col("PP_CLIMA") == 0, (F.col("PP") > 0).cast("int")))
         .withColumn("CLIMA_FUENTE", F.lit(fuente)))
    prev = s.select(*claves, (F.col("T") + 1).alias("T"), F.col("PP_ANOMALIA").alias("PP_ANOMALIA_LAG1"),
                    F.col("MES_LLUVIOSO").alias("MES_LLUVIOSO_LAG1"))
    s = s.join(prev, claves + ["T"], "left")
    r = s.agg(F.count("*").alias("n"), F.sum((F.col("PP_CLIMA") == 0).cast("int")).alias("clima0"),
              F.min("N_ANIOS_REF").alias("min_ref"), F.max("N_ANIOS_REF").alias("max_ref"),
              F.sum("MES_LLUVIOSO").alias("lluviosos")).first()
    print("%-8s serie=%d meses-unidad | anios de referencia por (unidad, mes): %s-%s | climatologia = 0 mm: %d | MES_LLUVIOSO=1: %d"
          % (fuente, r["n"], r["min_ref"], r["max_ref"], r["clima0"], r["lluviosos"]))
    return s.select(*claves, "ANIO", "MES", "PP_CLIMA", "PP_ANOMALIA", "MES_LLUVIOSO",
                    "PP_ANOMALIA_LAG1", "MES_LLUVIOSO_LAG1", "CLIMA_FUENTE")


def join_contado(izq, der, on, nombre):
    n_izq = izq.count()
    res = izq.join(der, on, "left")
    n_res = res.count()
    print("JOIN %-28s | izq=%6d | der=%6d (claves unicas=%6d) | resultado=%6d | %s"
          % (nombre, n_izq, der.count(), der.select(on).distinct().count(), n_res,
             "OK sin explosion" if n_res == n_izq else "ERROR"))
    assert n_res == n_izq
    return res


titulo("2b.1 SERIES DE PRECIPITACION Y ANOMALIA (referencia 2015-01 a 2021-12)")
panel = spark.read.parquet(C.PANEL)
sen = (spark.read.parquet(C.PROCESSED + "clima_distrito_mes.parquet")
       .groupBy("DEPTO_N", "ANIO", "MES").agg(F.avg("PP_TOTAL").alias("PP")))
nasa_raw = spark.read.csv(C.RAW + "clima/clima_nasa_la_libertad.csv", header=True, schema=ESQ_NASA)
print("NASA crudo: %d filas, %d distritos, %s a %s"
      % (nasa_raw.count(), nasa_raw.select("DIST_N").distinct().count(),
         *[("%d-%02d" % (r[0], r[1])) for r in [nasa_raw.orderBy("ANIO", "MES").select("ANIO", "MES").first(),
                                                 nasa_raw.orderBy(F.desc("ANIO"), F.desc("MES")).select("ANIO", "MES").first()]]))
nasa = nasa_raw.select("DEPTO_N", "DIST_N", "ANIO", "MES", F.col("PP_TOTAL_MM").alias("PP"))

an_sen = anomalia(sen, ["DEPTO_N"], "SENAMHI").filter(F.col("DEPTO_N") != LL)
an_nasa = anomalia(nasa, ["DEPTO_N", "DIST_N"], "NASA")

# control: la serie SENAMHI recalculada coincide con PP_TOTAL del panel
chk = (panel.filter(F.col("PP_TOTAL").isNotNull()).join(sen, ["DEPTO_N", "ANIO", "MES"], "left")
       .agg(F.max(F.abs(F.col("PP_TOTAL") - F.col("PP"))).alias("d")).first()["d"])
print("Control SENAMHI: max |PP_TOTAL panel - serie recalculada| = %s" % chk)

titulo("2b.2 JOIN AL PANEL")
cols_cli = ["PP_CLIMA", "PP_ANOMALIA", "MES_LLUVIOSO", "PP_ANOMALIA_LAG1", "MES_LLUVIOSO_LAG1", "CLIMA_FUENTE"]
p = join_contado(panel, an_sen, ["DEPTO_N", "ANIO", "MES"], "x SENAMHI (depto-mes)")
for c in cols_cli:
    p = p.withColumnRenamed(c, c + "_S")
p = join_contado(p, an_nasa, ["DEPTO_N", "DIST_N", "ANIO", "MES"], "x NASA LL (distrito-mes)")
for c in cols_cli:
    p = p.withColumn(c, F.coalesce(F.col(c), F.col(c + "_S"))).drop(c + "_S")
p = p.withColumn("CLIMA_DISPONIBLE", F.col("PP_ANOMALIA").isNotNull().cast("int"))
p = p.withColumn("CLIMA_FUENTE", F.coalesce(F.col("CLIMA_FUENTE"), F.lit("SIN_CLIMA"))).cache()

titulo("2b.3 COBERTURA DE CLIMA EN EL PANEL")
n = p.count()
print("Panel con clima: %d filas x %d columnas" % (n, len(p.columns)))
p.groupBy("CLIMA_FUENTE").agg(F.count("*").alias("filas"), F.countDistinct("DEPTO_N", "DIST_N").alias("distritos"),
                              F.sum("CLIMA_DISPONIBLE").alias("con_anomalia"),
                              F.sum(F.col("PP_ANOMALIA_LAG1").isNotNull().cast("int")).alias("con_anomalia_lag1"),
                              F.sum("MES_LLUVIOSO").alias("meses_lluviosos"),
                              F.round(F.avg("PP_ANOMALIA"), 3).alias("anomalia_media")).orderBy("CLIMA_FUENTE").show()
p.agg(F.sum("CLIMA_DISPONIBLE").alias("filas_con_clima"),
      F.round(F.avg("CLIMA_DISPONIBLE") * 100, 2).alias("pct_con_clima")).show()
p.filter(F.col("DEPTO_N") == LL).agg(F.count("*").alias("filas_LL"), F.countDistinct("DIST_N").alias("distritos_LL"),
                                     F.sum("CLIMA_DISPONIBLE").alias("filas_LL_con_clima"),
                                     F.round(F.avg("CLIMA_DISPONIBLE") * 100, 2).alias("pct_LL_con_clima")).show()
print("Tasa de interrupcion (TARGET) segun MES_LLUVIOSO_LAG1 (descriptivo, filas con clima):")
p.filter(F.col("MES_LLUVIOSO_LAG1").isNotNull()).groupBy("CLIMA_FUENTE", "MES_LLUVIOSO_LAG1").agg(
    F.count("*").alias("filas"), F.round(F.avg("TARGET"), 4).alias("tasa_TARGET")).orderBy("CLIMA_FUENTE", "MES_LLUVIOSO_LAG1").show()
print("La Libertad - meses con MES_LLUVIOSO=1 en el periodo del panel:")
(p.filter((F.col("DEPTO_N") == LL) & (F.col("MES_LLUVIOSO") == 1)).groupBy("PERIODO")
 .agg(F.count("*").alias("distritos"), F.round(F.avg("PP_ANOMALIA"), 2).alias("anomalia_media"))
 .orderBy("PERIODO").show(60, truncate=False))

titulo("2b.4 ESCRITURA")
p.repartition("ANIO").write.mode("overwrite").partitionBy("ANIO").parquet(PANEL_CLIMA)
print("Escrito %s | verificacion de lectura: %d filas" % (PANEL_CLIMA, spark.read.parquet(PANEL_CLIMA).count()))
spark.stop()
