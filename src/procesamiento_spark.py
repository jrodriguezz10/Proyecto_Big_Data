"""2. Procesamiento: limpieza, agregaciones y panel distrito-mes -> processed/master_panel.parquet

Ejecutar: docker exec spark_node /spark/bin/spark-submit /app/src/procesamiento_spark.py

Decisiones (ver diagnostico.py):
* Interrupciones: se eliminan filas identicas y luego duplicados (IDINTERRUPCION, departamento,
  distrito). Un mismo IDINTERRUPCION puede afectar a varios distritos, por eso no se deduplica
  solo por ID.
* SENAMHI solo tiene 5 estaciones (Arequipa, Lima-provincias, Tacna) y la coincidencia de
  DISTRITO con interrupciones es 0 %. El clima se agrega a distrito-mes (como se pide) y,
  para poder unirlo al panel, se lleva a DEPARTAMENTO-mes (promedio de las estaciones del
  departamento). Los departamentos sin estacion (p. ej. LA LIBERTAD) quedan con clima nulo y
  CLIMA_DISPONIBLE = 0: no se inventa precipitacion.
* El panel es: distritos con interrupciones reportadas x meses del periodo observado,
  LEFT JOIN clima, LEFT JOIN conteo de interrupciones, LEFT JOIN peligros / infraestructura.
"""
from pyspark.sql import Window, functions as F

import comun as C
from comun import norm, titulo

spark = C.spark_session("SEDALIB_Procesamiento")
sc = spark.sparkContext

TIPOS_PELIGRO_VALIDOS = [
    "CAIDA", "FLUJO", "DESLIZAMIENTO", "EROSION DE LADERA", "EROSION FLUVIAL",
    "INUNDACION FLUVIAL", "MOVIMIENTO COMPLEJO", "REPTACION DE SUELOS", "ARENAMIENTO",
    "HUNDIMIENTO", "VUELCO", "INUNDACION LAGUNAR", "EROSION MARINA", "INUNDACION PLUVIAL",
    "PROPAGACION LATERAL"]
KEY_D = ["DEPTO_N", "DIST_N"]


def join_contado(izq, der, on, how, nombre, bcast=False):
    """JOIN que imprime filas antes/despues y verifica que no haya explosion de filas."""
    n_izq = izq.count()
    n_der, n_claves = der.count(), der.select(on).distinct().count()
    res = izq.join(F.broadcast(der) if bcast else der, on, how)
    n_res = res.count()
    print("JOIN %-34s | izq=%7d | der=%6d (claves unicas=%6d) | resultado=%7d | %s"
          % (nombre, n_izq, n_der, n_claves, n_res,
             "OK sin explosion" if n_res == n_izq else "ERROR: cambio el numero de filas"))
    assert n_res == n_izq, "Explosion de filas en " + nombre
    return res


# ------------------------------------------------------------------ interrupciones
titulo("2.1 INTERRUPCIONES (SUNASS)")
int_raw = C.leer_interrupciones(spark)
n0 = int_raw.count()
inte = int_raw.dropDuplicates()
n1 = inte.count()
inte = (inte.withColumn("EPS", F.trim("EPS"))
        .withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO")))
        .withColumn("DIST_N", norm(F.col("DISTRITO")))
        .dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"]))
n2 = inte.count()
inte = (inte.withColumn("F_INI", F.to_date("FECHAINICIO", "yyyyMMdd"))
        .withColumn("F_FIN", F.to_date("FECHAFIN", "yyyyMMdd"))
        .withColumn("TS_INI", F.to_timestamp(F.concat_ws(" ", "FECHAINICIO", "HORAINICIO"), "yyyyMMdd H:mm"))
        .withColumn("TS_FIN", F.to_timestamp(F.concat_ws(" ", "FECHAFIN", "HORAFIN"), "yyyyMMdd H:mm")))
valida = (F.col("F_INI").isNotNull() & F.col("F_FIN").isNotNull() & (F.col("F_FIN") >= F.col("F_INI"))
          & F.col("DIST_N").isNotNull())
inte_ok = (inte.filter(valida)
           .withColumn("ANIO", F.year("F_INI")).withColumn("MES", F.month("F_INI"))
           .withColumn("TIEMPO_RESPUESTA_DIAS", F.datediff("F_FIN", "F_INI"))
           .withColumn("DURACION_HORAS", (F.unix_timestamp("TS_FIN") - F.unix_timestamp("TS_INI")) / 3600.0))
n3 = inte_ok.count()
print("filas raw                                   : %d" % n0)
print("tras dropDuplicates() (filas identicas)     : %d  (-%d)" % (n1, n0 - n1))
print("tras dropDuplicates(ID, depto, distrito)    : %d  (-%d)" % (n2, n1 - n2))
print("tras rechazar fecha invalida/distrito nulo  : %d  (-%d)" % (n3, n2 - n3))

int_dm = inte_ok.groupBy(*KEY_D, "ANIO", "MES").agg(
    F.countDistinct("IDINTERRUPCION").alias("N_INTERRUPCIONES"),
    F.countDistinct(F.when(F.col("TIPOINTERRUPCION") == "IMPREVISTA", F.col("IDINTERRUPCION"))).alias("N_IMPREVISTAS"))

# ------------------------------------------------------------------ clima
titulo("2.2 CLIMA (SENAMHI): imputacion y agregacion distrito-mes")
cli = C.leer_clima(spark)
c0 = cli.count()
cli = (cli.dropDuplicates()
       .withColumn("HORA_INT", F.col("HORA").cast("int"))
       .dropDuplicates(["ESTACION", "FECHA", "HORA_INT"])
       .withColumn("F_DIA", F.to_date("FECHA", "yyyyMMdd")))
c1 = cli.count()
cli = cli.filter(F.col("F_DIA").isNotNull()).withColumn("MES", F.month("F_DIA")).withColumn("ANIO", F.year("F_DIA"))
c2 = cli.count()
cli = (cli.withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO"))).withColumn("DIST_N", norm(F.col("DISTRITO")))
       .withColumn("PP_IMPUTADO", F.col("PP").isNull().cast("int"))
       .withColumn("PP", F.coalesce(F.col("PP"), F.lit(0.0))))
med_temp = cli.groupBy("ESTACION", "MES").agg(F.expr("percentile_approx(TEMP, 0.5, 10000)").alias("TEMP_MEDIANA"))
cli = (cli.join(F.broadcast(med_temp), ["ESTACION", "MES"], "left")
       .withColumn("TEMP_IMPUTADO", F.col("TEMP").isNull().cast("int"))
       .withColumn("TEMP", F.coalesce(F.col("TEMP"), F.col("TEMP_MEDIANA"))))
imp = cli.agg(F.sum("PP_IMPUTADO").alias("pp"), F.sum("TEMP_IMPUTADO").alias("t"),
              F.sum(F.col("TEMP").isNull().cast("int")).alias("t_rest")).first()
print("filas raw: %d | tras dropDuplicates (estacion, fecha, hora): %d | con fecha valida: %d" % (c0, c1, c2))
print("PP nulo -> 0.0 (PP_IMPUTADO=1)              : %d registros horarios" % imp["pp"])
print("TEMP nulo -> mediana estacion-mes           : %d registros (nulos restantes: %d)" % (imp["t"], imp["t_rest"]))

cli_dia = cli.groupBy(*KEY_D, "ESTACION", "ANIO", "MES", "F_DIA").agg(
    F.sum("PP").alias("PP_DIA"), F.avg("TEMP").alias("TEMP_DIA"),
    F.count("*").alias("N_REG"), F.sum("PP_IMPUTADO").alias("N_PP_IMP"))
cli_est_mes = cli_dia.groupBy(*KEY_D, "ESTACION", "ANIO", "MES").agg(
    F.sum("PP_DIA").alias("PP_TOTAL"), F.max("PP_DIA").alias("PP_MAX"),
    F.sum((F.col("PP_DIA") > 15).cast("int")).alias("DIAS_LLUVIA_ALTA"),
    F.avg("TEMP_DIA").alias("TEMP_MEDIA"),
    (F.sum("N_PP_IMP") / F.sum("N_REG")).alias("PCT_PP_IMPUTADO"))
clima_dm = cli_est_mes.groupBy(*KEY_D, "ANIO", "MES").agg(
    *[F.avg(c).alias(c) for c in ["PP_TOTAL", "PP_MAX", "DIAS_LLUVIA_ALTA", "TEMP_MEDIA", "PCT_PP_IMPUTADO"]])
clima_dm.write.mode("overwrite").parquet(C.PROCESSED + "clima_distrito_mes.parquet")
print("clima distrito-mes: %d filas (5 distritos con estacion)" % clima_dm.count())
clima_dm.groupBy("DEPTO_N", "DIST_N").agg(
    F.count("*").alias("meses"), F.round(F.avg("PP_TOTAL"), 2).alias("PP_TOTAL_medio"),
    F.round(F.max("PP_MAX"), 2).alias("PP_MAX_diario"), F.sum("DIAS_LLUVIA_ALTA").alias("dias_>15mm")).show(truncate=False)
clima_depm = clima_dm.groupBy("DEPTO_N", "ANIO", "MES").agg(
    *[F.avg(c).alias(c) for c in ["PP_TOTAL", "PP_MAX", "DIAS_LLUVIA_ALTA", "TEMP_MEDIA", "PCT_PP_IMPUTADO"]],
    F.count("*").alias("N_ESTACIONES"))

# ------------------------------------------------------------------ peligros
titulo("2.3 PELIGROS (INGEMMET) agregados por distrito ANTES del join")
pel = C.leer_peligros(spark)
p0 = pel.count()
pel = (pel.withColumn("TIPO", norm(F.col("TIP_PELIGRO")))
       .withColumn("DEPTO_N", norm(F.col("NM_DPTO"))).withColumn("DIST_N", norm(F.col("NM_DIST")))
       .filter(F.col("TIPO").isin(TIPOS_PELIGRO_VALIDOS) & F.col("DIST_N").isNotNull())
       .dropDuplicates())
p1 = pel.count()
print("filas raw: %d | validas y sin duplicados: %d (rechazadas %d por fila mal formada/tipo invalido)" % (p0, p1, p0 - p1))
por_tipo = pel.groupBy(*KEY_D, "TIPO").count()
w_tipo = Window.partitionBy(*KEY_D).orderBy(F.desc("count"), "TIPO")
pel_agg = (por_tipo.withColumn("rk", F.row_number().over(w_tipo))
           .groupBy(*KEY_D).agg(F.sum("count").alias("N_PELIGROS"),
                                F.max(F.when(F.col("rk") == 1, F.col("TIPO"))).alias("TIPO_PELIGRO_PRINCIPAL")))
print("distritos con peligros inventariados: %d" % pel_agg.count())

# ------------------------------------------------------------------ infraestructura
titulo("2.4 INFRAESTRUCTURA (DATASS): antiguedad y estado por distrito")
ant = C.leer_infra(spark, "datass_antiguedad").dropDuplicates() \
    .withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO"))).withColumn("DIST_N", norm(F.col("DISTRITO")))
est = C.leer_infra(spark, "datass_estado").dropDuplicates() \
    .withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO"))).withColumn("DIST_N", norm(F.col("DISTRITO")))
ant_agg = ant.groupBy(*KEY_D).agg(
    F.count("*").alias("N_SISTEMAS_AGUA"),
    F.round(F.avg(norm(F.col("VALOR")).contains("MAS DE 20").cast("double")) * 100, 2).alias("PCT_SIST_MAS_20_ANIOS"))
est_agg = est.groupBy(*KEY_D).agg(
    F.round(F.avg((norm(F.col("VALOR")) == "NO OPERA").cast("double")) * 100, 2).alias("PCT_SIST_NO_OPERA"))
infra_agg = ant_agg.join(est_agg, KEY_D, "full_outer")
print("DATASS no trae la edad en anios: solo la categoria '<=20 anios' / '>20 anios' por sistema.")
print("Por eso la 'antiguedad promedio' se expresa como % de sistemas con mas de 20 anios.")
print("distritos con datos de infraestructura: %d" % infra_agg.count())

# ------------------------------------------------------------------ panel
titulo("2.5 PANEL DISTRITO-MES (filas antes y despues de cada JOIN)")
rango = inte_ok.agg(F.min("F_INI").alias("a"), F.max("F_INI").alias("b")).first()
meses = (spark.range(1).select(F.explode(F.sequence(F.trunc(F.lit(rango["a"]), "month"),
                                                     F.trunc(F.lit(rango["b"]), "month"),
                                                     F.expr("interval 1 month"))).alias("M"))
         .select(F.year("M").alias("ANIO"), F.month("M").alias("MES")))
distritos = inte_ok.select(*KEY_D).distinct()
base = distritos.crossJoin(meses)
print("distritos con interrupciones: %d | meses %s a %s: %d | base = %d filas"
      % (distritos.count(), rango["a"].strftime("%Y-%m"), rango["b"].strftime("%Y-%m"),
         meses.count(), base.count()))

panel = join_contado(base, clima_depm, ["DEPTO_N", "ANIO", "MES"], "left", "base x clima (depto-mes)")
panel = join_contado(panel, int_dm, KEY_D + ["ANIO", "MES"], "left", "x conteo interrupciones")
panel = join_contado(panel, pel_agg, KEY_D, "left", "x peligros (broadcast)", bcast=True)
panel = join_contado(panel, infra_agg, KEY_D, "left", "x infraestructura (broadcast)", bcast=True)

w = Window.partitionBy(*KEY_D).orderBy("ANIO", "MES")
panel = (panel.fillna(0, subset=["N_INTERRUPCIONES", "N_IMPREVISTAS", "N_PELIGROS"])
         .withColumn("TARGET", (F.col("N_INTERRUPCIONES") > 0).cast("int"))
         .withColumn("CLIMA_DISPONIBLE", F.col("PP_TOTAL").isNotNull().cast("int"))
         .withColumn("INFRA_DISPONIBLE", F.col("N_SISTEMAS_AGUA").isNotNull().cast("int"))
         .withColumn("PERIODO", F.format_string("%d-%02d", F.col("ANIO"), F.col("MES")))
         .withColumn("INT_LAG1", F.lag("N_INTERRUPCIONES", 1).over(w))
         .withColumn("TARGET_LAG1", F.lag("TARGET", 1).over(w))
         .withColumn("INT_PREV3", F.sum("N_INTERRUPCIONES").over(w.rowsBetween(-3, -1)))
         .withColumn("PP_TOTAL_LAG1", F.lag("PP_TOTAL", 1).over(w))
         .withColumn("PP_MAX_LAG1", F.lag("PP_MAX", 1).over(w))
         .withColumn("DIAS_LLUVIA_ALTA_LAG1", F.lag("DIAS_LLUVIA_ALTA", 1).over(w)))

n_panel = panel.count()
print("\nPanel final: %d filas x %d columnas" % (n_panel, len(panel.columns)))
panel.agg(F.sum("TARGET").alias("meses_con_interrupcion"), F.round(F.avg("TARGET"), 4).alias("prevalencia_TARGET"),
          F.sum("N_INTERRUPCIONES").alias("interrupciones_contadas"),
          F.sum("CLIMA_DISPONIBLE").alias("filas_con_clima"),
          F.sum("INFRA_DISPONIBLE").alias("filas_con_infra")).show(truncate=False)
panel.filter(F.col("DEPTO_N") == "LA LIBERTAD").agg(
    F.countDistinct("DIST_N").alias("distritos_LL"), F.count("*").alias("filas_LL"),
    F.sum("N_INTERRUPCIONES").alias("interrupciones_LL"), F.sum("CLIMA_DISPONIBLE").alias("filas_LL_con_clima")).show()
print(panel.columns)

titulo("2.6 ESCRITURA PARQUET particionado por ANIO")
panel.repartition("ANIO").write.mode("overwrite").partitionBy("ANIO").parquet(C.PANEL)
jvm = sc._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
for st in fs.listStatus(jvm.org.apache.hadoop.fs.Path(C.PANEL)):
    print("  %s %s" % ("DIR " if st.isDirectory() else "FILE", st.getPath().toString()))
print("Verificacion de lectura: %d filas" % spark.read.parquet(C.PANEL).count())
spark.stop()
