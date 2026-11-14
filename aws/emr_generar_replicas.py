"""Genera en EMR las replicas NX de interrupciones con el MISMO metodo de src/escalabilidad_spark.py:
crossJoin(spark.range(N)) + sufijo "_<rep>" en IDINTERRUPCION (las demas columnas no cambian).

Lee raw/ real (solo lectura) y escribe CSV (cabecera, mismo esquema y separador) en
s3://<bucket>/escalabilidad/<N>X/raw/operaciones/sunass_interrupciones.csv/
Las replicas son solo para medir rendimiento: no se escriben en raw/ ni processed/ y no alimentan el modelo.

    ESCALAS_REPLICA="100,500" (variable de entorno)    SEDALIB_DATALAKE=s3://<bucket>/
"""
import os
import time

from pyspark.sql import functions as F

import comun as C

spark = C.spark_session("SEDALIB_GenerarReplicas")
base = C.leer_interrupciones(spark).cache()
n_base = base.count()
print("Filas base (raw/operaciones/sunass_interrupciones.csv): %d" % n_base)
for n in [int(x) for x in os.environ["ESCALAS_REPLICA"].split(",")]:
    destino = C.DATALAKE + "escalabilidad/%dX/raw/operaciones/sunass_interrupciones.csv" % n
    t0 = time.perf_counter()
    rep = (base.crossJoin(spark.range(n).withColumnRenamed("id", "REP"))
           .withColumn("IDINTERRUPCION", F.concat_ws("_", "IDINTERRUPCION", F.col("REP").cast("string")))
           .drop("REP"))
    rep.write.mode("overwrite").option("header", True).csv(destino)
    print("[replica] %dX | filas=%d | %.2f s | %s" % (n, n_base * n, time.perf_counter() - t0,
                                                   destino.replace(C.HDFS, "s3://<bucket>")), flush=True)
spark.stop()
