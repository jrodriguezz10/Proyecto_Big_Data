"""4. Optimizacion: SortMergeJoin (linea base) vs BroadcastHashJoin.

Consulta medida (datos reales, sin replicar): interrupciones limpias a nivel evento
JOIN clima depto-mes JOIN peligros por distrito JOIN infraestructura por distrito,
agregada a distrito-mes. Las entradas se materializan en memoria (localCheckpoint) antes
de medir para aislar el costo del JOIN (no la lectura CSV) y acortar el plan.
Linea base: spark.sql.autoBroadcastJoinThreshold=-1 (SortMergeJoin).
Diseno 2x2: tipo de JOIN (SortMerge / Broadcast) x spark.sql.shuffle.partitions (200 / 16).
1 corrida de calentamiento + 3 corridas medidas por modo; se reporta la mediana.

Ejecutar: docker exec spark_node /spark/bin/spark-submit /app/src/optimizacion_spark.py
Variable opcional SPARK_UI_HOLD=<segundos>: mantiene viva la Spark UI (puerto 4040) al final.
"""
import os
import statistics
import time

from pyspark.sql import functions as F

import comun as C
from comun import norm, titulo

spark = C.spark_session("SEDALIB_Optimizacion", **{"spark.sql.autoBroadcastJoinThreshold": "-1"})
sc = spark.sparkContext
KEY_D = ["DEPTO_N", "DIST_N"]

# ---------------------------------------------------------------- entradas materializadas
inte = (C.leer_interrupciones(spark).dropDuplicates()
        .withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO"))).withColumn("DIST_N", norm(F.col("DISTRITO")))
        .dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"])
        .withColumn("F_INI", F.to_date("FECHAINICIO", "yyyyMMdd"))
        .filter(F.col("F_INI").isNotNull() & F.col("DIST_N").isNotNull())
        .withColumn("ANIO", F.year("F_INI")).withColumn("MES", F.month("F_INI"))
        .select("IDINTERRUPCION", "TIPOINTERRUPCION", *KEY_D, "ANIO", "MES")).localCheckpoint()
clima_depm = (spark.read.parquet(C.PROCESSED + "clima_distrito_mes.parquet")
              .groupBy("DEPTO_N", "ANIO", "MES").agg(F.avg("PP_TOTAL").alias("PP_TOTAL"),
                                                     F.avg("DIAS_LLUVIA_ALTA").alias("DIAS_LLUVIA_ALTA"))).localCheckpoint()
pel_agg = (C.leer_peligros(spark).withColumn("DEPTO_N", norm(F.col("NM_DPTO")))
           .withColumn("DIST_N", norm(F.col("NM_DIST"))).withColumn("TIPO", norm(F.col("TIP_PELIGRO")))
           .filter(F.col("DIST_N").isNotNull() & F.col("TIPO").isNotNull())
           .groupBy(*KEY_D).agg(F.count("*").alias("N_PELIGROS"))).localCheckpoint()
infra_agg = (C.leer_infra(spark, "datass_antiguedad").withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO")))
             .withColumn("DIST_N", norm(F.col("DISTRITO")))
             .groupBy(*KEY_D).agg(F.avg(norm(F.col("VALOR")).contains("MAS DE 20").cast("double")).alias("PCT_MAS_20"))
             ).localCheckpoint()
tam = {n: d.count() for n, d in [("interrupciones", inte), ("clima depto-mes", clima_depm),
                                 ("peligros por distrito", pel_agg), ("infraestructura por distrito", infra_agg)]}

titulo("4. PRUEBA DE RENDIMIENTO: SortMergeJoin vs BroadcastHashJoin")
print("Filas de entrada (materializadas): %s" % tam)
print("spark.sql.autoBroadcastJoinThreshold = %s | AQE = %s | cores = %d"
      % (spark.conf.get("spark.sql.autoBroadcastJoinThreshold"), spark.conf.get("spark.sql.adaptive.enabled"),
         sc.defaultParallelism))


def consulta(bcast):
    b = F.broadcast if bcast else (lambda d: d)
    return (inte.join(b(clima_depm), ["DEPTO_N", "ANIO", "MES"], "left")
            .join(b(pel_agg), KEY_D, "left")
            .join(b(infra_agg), KEY_D, "left")
            .groupBy(*KEY_D, "ANIO", "MES")
            .agg(F.count("*").alias("N_INT"), F.avg("PP_TOTAL").alias("PP_TOTAL"),
                 F.max("N_PELIGROS").alias("N_PELIGROS"), F.max("PCT_MAS_20").alias("PCT_MAS_20")))


def medir(nombre, bcast, particiones):
    spark.conf.set("spark.sql.shuffle.partitions", str(particiones))
    tiempos, filas = [], 0
    for i in range(4):  # 0 = calentamiento
        sc.setJobGroup("opt_%s_%d" % (nombre, i), "%s corrida %d" % (nombre, i))
        df = consulta(bcast)
        t0 = time.perf_counter()
        filas = len(df.collect())
        tiempos.append(time.perf_counter() - t0)
    plan = sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")
    return tiempos, filas, plan.split("+- == Initial Plan ==")[0]


MODOS = [("A_SortMerge_200part", False, 200), ("B_Broadcast_200part", True, 200),
         ("C_SortMerge_16part", False, 16), ("D_Broadcast_16part", True, 16)]
resultados = {nombre: medir(nombre, bcast, part) for nombre, bcast, part in MODOS}

titulo("RESUMEN (1 corrida de calentamiento + 3 medidas; se reporta la mediana)")
print("%-20s %8s | %-23s | %8s | %6s | %s" % ("modo", "calent.", "corridas medidas (s)", "mediana", "filas", "joins en el plan final"))
med = {}
for nombre, (tiempos, filas, plan) in resultados.items():
    med[nombre] = statistics.median(tiempos[1:])
    joins = {j: plan.count(j) for j in ["SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin"] if plan.count(j)}
    print("%-20s %6.3f s | %-23s | %6.3f s | %6d | %s" % (nombre, tiempos[0], ", ".join("%.3f" % t for t in tiempos[1:]),
                                                     med[nombre], filas, joins))
a = med["A_SortMerge_200part"]
print("\nReduccion de tiempo respecto a la linea base A (negativo = mas lento):")
for nombre in ["B_Broadcast_200part", "C_SortMerge_16part", "D_Broadcast_16part"]:
    b = med[nombre]
    print("   %-22s %+6.1f %%  (%.3f s -> %.3f s, speedup %.2fx)" % (nombre, (a - b) * 100 / a, a, b, a / b))

for nombre in ["A_SortMerge_200part", "B_Broadcast_200part"]:
    titulo("explain() plan fisico final - " + nombre)
    print(resultados[nombre][2])

hold = int(os.environ.get("SPARK_UI_HOLD", "0"))
if hold:
    print("\nSpark UI disponible en http://localhost:4040/SQL/ durante %d s" % hold, flush=True)
    fin = time.time() + hold
    while time.time() < fin and not os.path.exists("/app/evidencias/.liberar_ui"):
        time.sleep(1)
spark.stop()
