"""1. Linea base de SEDALIB (seccion 1.4 del informe).

Ejecutar: docker exec spark_node /spark/bin/spark-submit /app/src/linea_base.py
"""
from pyspark.sql import functions as F

import comun as C
from comun import titulo

spark = C.spark_session("SEDALIB_LineaBase")

titulo("1.1 AGUA NO FACTURADA (sunass_anf.csv)")
anf = C.leer_anf(spark)
print("Columnas disponibles: %s" % anf.columns)
anf.filter(F.col("EPS").contains("SEDALIB")).show(truncate=False)
anf.select(F.round(F.avg("AGUA_NO_FACTURADA_PORCENTAJE"), 2).alias("ANF_promedio_16_EPS"),
           F.min("AGUA_NO_FACTURADA_PORCENTAJE").alias("min"),
           F.max("AGUA_NO_FACTURADA_PORCENTAJE").alias("max")).show()
print("ANIO del indicador : NO EXISTE en el archivo (no hay columna de anio/periodo).")
print("Volumen ANF (m3)   : NO EXISTE en el archivo (solo trae el porcentaje).")

titulo("1.2 INTERRUPCIONES DE SEDALIB")
inte = (C.leer_interrupciones(spark).withColumn("EPS", F.trim("EPS"))
        .filter(F.col("EPS").contains("SEDALIB")).dropDuplicates())
inte = (inte.withColumn("F_INI", F.to_date("FECHAINICIO", "yyyyMMdd"))
        .withColumn("F_FIN", F.to_date("FECHAFIN", "yyyyMMdd"))
        .withColumn("TS_INI", F.to_timestamp(F.concat_ws(" ", "FECHAINICIO", "HORAINICIO"), "yyyyMMdd H:mm"))
        .withColumn("TS_FIN", F.to_timestamp(F.concat_ws(" ", "FECHAFIN", "HORAFIN"), "yyyyMMdd H:mm"))
        .withColumn("ANIO", F.year("F_INI"))
        .withColumn("TIEMPO_RESPUESTA_DIAS", F.datediff("F_FIN", "F_INI"))
        .withColumn("DURACION_HORAS", (F.unix_timestamp("TS_FIN") - F.unix_timestamp("TS_INI")) / 3600.0))
print("Filas SEDALIB (sin filas identicas): %d | eventos (IDINTERRUPCION distintos): %d"
      % (inte.count(), inte.select("IDINTERRUPCION").distinct().count()))
print("Rango de fechas: %s" % (inte.agg(F.min("F_INI"), F.max("F_INI")).first(),))
print("\nInterrupciones de SEDALIB por anio (eventos distintos):")
inte.groupBy("ANIO").agg(F.countDistinct("IDINTERRUPCION").alias("eventos"),
                         F.count("*").alias("filas_evento_distrito")).orderBy("ANIO").show()
print("Nota: 2019 sin registros de SEDALIB y 2024 solo hasta mayo (corte del archivo).")

ev = inte.dropDuplicates(["IDINTERRUPCION"])
titulo("1.3 TIEMPO DE RESPUESTA (por evento)")
ev.agg(F.expr("percentile(TIEMPO_RESPUESTA_DIAS, 0.5)").alias("mediana_TIEMPO_RESPUESTA_DIAS"),
       F.round(F.avg("TIEMPO_RESPUESTA_DIAS"), 3).alias("media_dias"),
       F.max("TIEMPO_RESPUESTA_DIAS").alias("max_dias"),
       F.expr("percentile(DURACION_HORAS, 0.5)").alias("mediana_DURACION_HORAS"),
       F.round(F.avg("DURACION_HORAS"), 2).alias("media_horas")).show(truncate=False)
print("TIEMPO_RESPUESTA_DIAS = FECHAFIN - FECHAINICIO (dias calendario);"
      " DURACION_HORAS usa tambien HORAINICIO/HORAFIN.")
ev.groupBy("TIEMPO_RESPUESTA_DIAS").count().orderBy("TIEMPO_RESPUESTA_DIAS").show()

titulo("1.4 PROGRAMADAS vs IMPREVISTAS (columna TIPOINTERRUPCION)")
n_ev = ev.count()
ev.groupBy("TIPOINTERRUPCION").count() \
    .withColumn("pct", F.round(F.col("count") * 100 / n_ev, 2)).orderBy(F.desc("count")).show()
ev.groupBy("ANIO", "TIPOINTERRUPCION").count().orderBy("ANIO", "TIPOINTERRUPCION").show(30)
print("Top 5 motivos de interrupcion SEDALIB:")
ev.groupBy("MOTIVOINTERRUPCION").count().orderBy(F.desc("count")).show(5, truncate=False)
spark.stop()
