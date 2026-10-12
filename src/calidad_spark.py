"""3. Control de calidad: 5 reglas con conteos reales + cardinalidad de los JOINs del panel.

Ejecutar (despues de procesamiento_spark.py):
    docker exec spark_node /spark/bin/spark-submit /app/src/calidad_spark.py
"""
from pyspark.sql import functions as F

import comun as C
from comun import norm, titulo

spark = C.spark_session("SEDALIB_Calidad")
KEY_D = ["DEPTO_N", "DIST_N"]

inte = (C.leer_interrupciones(spark).withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO")))
        .withColumn("DIST_N", norm(F.col("DISTRITO"))))
cli = C.leer_clima(spark)
pel = C.leer_peligros(spark)
panel = spark.read.parquet(C.PANEL)
n_int, n_cli = inte.count(), cli.count()


def pct(a, b):
    return 100.0 * a / b if b else 0.0


titulo("REPORTE DE CALIDAD DE DATOS - DATALAKE SEDALIB")

# R1 -----------------------------------------------------------------------
n_ident = n_int - inte.dropDuplicates().count()
n_clave = inte.dropDuplicates().count() - inte.dropDuplicates().dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"]).count()
n_id_multi = inte.dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"]).groupBy("IDINTERRUPCION").count() \
    .filter("count > 1").count()
n_cli_dup = n_cli - cli.withColumn("H", F.col("HORA").cast("int")).dropDuplicates(["ESTACION", "FECHA", "H"]).count()
print("R1 UNICIDAD")
print("   interrupciones: %d filas | filas identicas eliminadas: %d | duplicados (ID, depto, distrito): %d"
      % (n_int, n_ident, n_clave))
print("   IDINTERRUPCION que afectan a >1 distrito (se conservan, no son duplicados): %d" % n_id_multi)
print("   clima: duplicados (estacion, fecha, hora): %d de %d" % (n_cli_dup, n_cli))

# R2 -----------------------------------------------------------------------
print("\nR2 COMPLETITUD (nulos en columnas clave)")
for nombre, df, cols, n in [("interrupciones", inte, ["IDINTERRUPCION", "FECHAINICIO", "FECHAFIN", "DISTRITO"], n_int),
                            ("clima", cli, ["FECHA", "PP", "TEMP", "HR"], n_cli)]:
    fila = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    for c in cols:
        print("   %-15s %-15s nulos=%7d (%5.2f %%)" % (nombre, c, fila[c], pct(fila[c], n)))
print("   Accion: DISTRITO nulo -> fila rechazada; PP nulo -> 0.0 con PP_IMPUTADO=1;"
      " TEMP nulo -> mediana estacion-mes.")

# R3 -----------------------------------------------------------------------
fi, ff = F.to_date("FECHAINICIO", "yyyyMMdd"), F.to_date("FECHAFIN", "yyyyMMdd")
n_fi = inte.filter(fi.isNull()).count()
n_ff = inte.filter(ff.isNull()).count()
n_inv = inte.filter(ff < fi).count()
n_cf = cli.filter(F.to_date("FECHA", "yyyyMMdd").isNull()).count()
n_pf = pel.filter(F.col("FECHA").isNotNull() & F.to_date("FECHA", "dd/MM/yyyy").isNull()).count()
print("\nR3 FECHAS INVALIDAS (rechazadas)")
print("   interrupciones FECHAINICIO no parseable (yyyyMMdd): %d" % n_fi)
print("   interrupciones FECHAFIN no parseable (yyyyMMdd)   : %d" % n_ff)
print("   interrupciones con FECHAFIN < FECHAINICIO         : %d" % n_inv)
print("   clima FECHA no parseable (yyyyMMdd)               : %d" % n_cf)
print("   peligros FECHA no parseable (dd/MM/yyyy)          : %d (no se usa en el panel)" % n_pf)

# R4 -----------------------------------------------------------------------
dist_int = inte.filter(F.col("DIST_N").isNotNull()).select(*KEY_D).distinct()
n_d = dist_int.count()
pel_d = pel.select(norm(F.col("NM_DPTO")).alias("DEPTO_N"), norm(F.col("NM_DIST")).alias("DIST_N")).distinct()
ant_d = C.leer_infra(spark, "datass_antiguedad").select(norm(F.col("DEPARTAMENTO")).alias("DEPTO_N"),
                                                        norm(F.col("DISTRITO")).alias("DIST_N")).distinct()
cli_d = cli.select(norm(F.col("DEPARTAMENTO")).alias("DEPTO_N"), norm(F.col("DISTRITO")).alias("DIST_N")).distinct()
cli_dep = cli_d.select("DEPTO_N").distinct()
print("\nR4 INTEGRIDAD REFERENCIAL de distritos (base: %d pares depto-distrito de interrupciones)" % n_d)
for nombre, ref, on in [("peligros INGEMMET (depto+distrito)", pel_d, KEY_D),
                        ("infraestructura DATASS (depto+distrito)", ant_d, KEY_D),
                        ("clima SENAMHI (depto+distrito)", cli_d, KEY_D),
                        ("clima SENAMHI (solo departamento)", cli_dep, ["DEPTO_N"])]:
    m = dist_int.join(ref, on, "left_semi").count()
    print("   %-42s %4d / %d = %6.2f %%" % (nombre, m, n_d, pct(m, n_d)))

# R5 -----------------------------------------------------------------------
fuera = cli.filter((F.col("PP") < 0) | (F.col("PP") > 500)).count()
cli_dia = cli.groupBy("ESTACION", "FECHA").agg(F.sum("PP").alias("PP_DIA"))
fuera_d = cli_dia.filter((F.col("PP_DIA") < 0) | (F.col("PP_DIA") > 500)).count()
rng = cli.agg(F.min("PP"), F.max("PP")).first()
print("\nR5 RANGO DE PRECIPITACION (0 <= PP <= 500 mm)")
print("   registros horarios fuera de rango: %d de %d (min=%s, max=%s)" % (fuera, n_cli, rng[0], rng[1]))
print("   dias-estacion fuera de rango     : %d de %d" % (fuera_d, cli_dia.count()))

# Cardinalidad -------------------------------------------------------------
titulo("VERIFICACION DE CARDINALIDAD DE LOS JOINs (panel master_panel.parquet)")
n_p = panel.count()
n_k = panel.select(*KEY_D, "ANIO", "MES").distinct().count()
n_dist = panel.select(*KEY_D).distinct().count()
n_mes = panel.select("ANIO", "MES").distinct().count()
n_limpias = (inte.dropDuplicates().dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"])
             .filter(fi.isNotNull() & ff.isNotNull() & (ff >= fi) & F.col("DIST_N").isNotNull()).count())
s_int = panel.agg(F.sum("N_INTERRUPCIONES")).first()[0]
print("   filas del panel                          : %d" % n_p)
print("   claves unicas (depto, distrito, anio, mes): %d -> %s" % (n_k, "OK 1 fila por clave" if n_k == n_p else "ERROR"))
print("   distritos x meses = %d x %d = %d -> %s" % (n_dist, n_mes, n_dist * n_mes,
                                                    "OK" if n_dist * n_mes == n_p else "ERROR"))
print("   suma N_INTERRUPCIONES en panel = %d | interrupciones limpias = %d -> %s"
      % (s_int, n_limpias, "OK no se pierden ni duplican" if s_int == n_limpias else "ERROR"))
print("   filas por departamento (top 5):")
panel.groupBy("DEPTO_N").count().orderBy(F.desc("count")).show(5)
spark.stop()
