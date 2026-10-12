"""0. Diagnostico de las fuentes raw y de los JOINs de la version anterior.

Ejecutar: docker exec spark_node /spark/bin/spark-submit /app/src/diagnostico.py
"""
from pyspark.sql import functions as F

import comun as C
from comun import norm, titulo

spark = C.spark_session("SEDALIB_Diagnostico")

fuentes = {
    "operaciones/sunass_interrupciones.csv": C.leer_interrupciones(spark),
    "operaciones/sunass_anf.csv": C.leer_anf(spark),
    "operaciones/sunass_hidrojets.csv": C.leer_hidrojets(spark),
    "clima/senamhi_clima.csv": C.leer_clima(spark),
    "geologia/ingemmet_peligros.csv (189 col. en origen)": C.leer_peligros(spark),
    "infraestructura/datass_antiguedad.csv (desde .xlsx)": C.leer_infra(spark, "datass_antiguedad"),
    "infraestructura/datass_estado.csv (desde .xlsx)": C.leer_infra(spark, "datass_estado"),
}

titulo("0.1 FILAS Y COLUMNAS DE CADA ARCHIVO RAW (HDFS)")
print("%-55s %10s %8s" % ("archivo", "filas", "columnas"))
for nombre, df in fuentes.items():
    print("%-55s %10d %8d" % (nombre, df.count(), len(df.columns)))

inte = fuentes["operaciones/sunass_interrupciones.csv"].withColumn("EPS", F.trim("EPS"))
total = inte.count()

titulo("0.2 INTERRUPCIONES POR EPS (top 15) Y POR DEPARTAMENTO")
inte.groupBy("EPS").count().withColumn("pct", F.round(F.col("count") * 100 / total, 2)) \
    .orderBy(F.desc("count")).show(15, truncate=False)
inte.groupBy("DEPARTAMENTO").count().withColumn("pct", F.round(F.col("count") * 100 / total, 2)) \
    .orderBy(F.desc("count")).show(30, truncate=False)
n_sedalib = inte.filter(F.col("EPS").contains("SEDALIB")).count()
n_ll = inte.filter(norm(F.col("DEPARTAMENTO")) == "LA LIBERTAD").count()
print("Total filas interrupciones          : %d" % total)
print("Filas de SEDALIB                    : %d (%.2f %%)" % (n_sedalib, n_sedalib * 100 / total))
print("Filas del departamento LA LIBERTAD  : %d (%.2f %%)" % (n_ll, n_ll * 100 / total))

titulo("0.3 PRECIPITACION (PP) EN SENAMHI")
clima = fuentes["clima/senamhi_clima.csv"]
clima.agg(F.countDistinct("PP").alias("valores_distintos_PP"), F.min("PP").alias("PP_min"),
          F.max("PP").alias("PP_max"), F.round(F.avg("PP"), 4).alias("PP_media"),
          F.sum(F.col("PP").isNull().cast("int")).alias("PP_nulos"),
          F.sum((F.col("PP") == 0).cast("int")).alias("PP_igual_0"),
          F.min("FECHA").alias("fecha_min"), F.max("FECHA").alias("fecha_max")).show(truncate=False)
print("Estaciones y distritos cubiertos por SENAMHI:")
clima.groupBy("DEPARTAMENTO", "PROVINCIA", "DISTRITO", "ESTACION").count() \
    .orderBy("DEPARTAMENTO").show(truncate=False)

titulo("0.4 % DE COINCIDENCIA DE DISTRITO ENTRE FUENTES (base: distritos de interrupciones)")
d_int_raw = inte.select(F.col("DISTRITO").alias("D")).dropna().distinct()
d_int_n = inte.select(norm(F.col("DISTRITO")).alias("D")).dropna().distinct()
otras = {
    "clima (SENAMHI)": clima.select("DISTRITO"),
    "peligros (INGEMMET NM_DIST)": fuentes["geologia/ingemmet_peligros.csv (189 col. en origen)"]
    .select(F.col("NM_DIST").alias("DISTRITO")),
    "antiguedad (DATASS)": fuentes["infraestructura/datass_antiguedad.csv (desde .xlsx)"].select("DISTRITO"),
}
n_raw, n_n = d_int_raw.count(), d_int_n.count()
print("Distritos distintos en interrupciones: %d (crudos) / %d (normalizados)" % (n_raw, n_n))
print("%-30s %22s %22s" % ("fuente", "coincide texto crudo", "coincide normalizado"))
for nombre, df in otras.items():
    o_raw = df.select(F.col("DISTRITO").alias("D")).dropna().distinct()
    o_n = df.select(norm(F.col("DISTRITO")).alias("D")).dropna().distinct()
    c_raw = d_int_raw.join(o_raw, "D").count()
    c_n = d_int_n.join(o_n, "D").count()
    print("%-30s %8d (%6.2f %%) %10d (%6.2f %%)" % (nombre, c_raw, c_raw * 100 / n_raw,
                                                    c_n, c_n * 100 / n_n))

titulo("0.5 FILAS ANTES Y DESPUES DE LOS JOINs DE LA VERSION ANTERIOR")
cli_raw = C.leer_clima(spark)
pel_raw = C.leer_peligros(spark)

print("(A) analisis_modelo.py anterior: interrupciones JOIN clima ON DISTRITO JOIN peligros ON DISTRITO")
a0 = inte.count()
a1_df = inte.join(cli_raw, on=["DISTRITO"], how="left")
a1 = a1_df.count()
a2_df = a1_df.join(pel_raw.withColumnRenamed("NM_DIST", "DISTRITO"), on=["DISTRITO"], how="left")
a2 = a2_df.count()
match_cli = a1_df.filter(F.col("ESTACION").isNotNull()).count()
match_pel = a2_df.filter(F.col("NOM_PELIGRO").isNotNull()).count()
print("   antes                 : %d filas" % a0)
print("   despues JOIN clima    : %d filas | filas con clima encontrado   : %d" % (a1, match_cli))
print("   despues JOIN peligros : %d filas | filas con peligro encontrado : %d" % (a2, match_pel))

print("\n(B) procesamiento_spark.py anterior: JOIN clima ON (DISTRITO, FECHA_CORTE) y peligros ON DISTRITO (upper/trim)")
b_int = inte.withColumn("DISTRITO", F.trim(F.upper("DISTRITO"))) \
    .withColumn("FECHA_CORTE", F.to_date(F.col("FECHAINICIO"), "yyyy-MM-dd"))
b_cli = cli_raw.withColumn("DISTRITO", F.trim(F.upper("DISTRITO"))) \
    .withColumn("FECHA_CORTE", F.to_date(F.col("FECHA"), "yyyy-MM-dd")).drop("FECHA")
b_pel = pel_raw.withColumn("DISTRITO", F.trim(F.upper("NM_DIST"))).drop("FECHA")
print("   FECHA_CORTE no nula con formato 'yyyy-MM-dd' sobre '20240418': %d de %d"
      % (b_int.filter(F.col("FECHA_CORTE").isNotNull()).count(), b_int.count()))
b1 = b_int.join(b_cli, ["DISTRITO", "FECHA_CORTE"], "left_outer")
b2 = b1.join(b_pel, ["DISTRITO"], "left_outer")
print("   antes                 : %d filas" % b_int.count())
print("   despues JOIN clima    : %d filas" % b1.count())
print("   despues JOIN peligros : %d filas  <-- explosion por JOIN muchos-a-muchos" % b2.count())
print("   (cada interrupcion se repite tantas veces como peligros tenga su distrito)")

titulo("0.6 VALORES QUE SALIAN EN LOS GRAFICOS ANTERIORES (reproduccion)")
a_fill = a2_df.fillna({"NOM_PELIGRO": "Sismo/Huaico", "PP": 12.5})
print("Top 5 distritos (sin filtrar EPS/departamento):")
a_fill.groupBy("DISTRITO").count().orderBy(F.desc("count")).show(5, truncate=False)
print("PP tras fillna(12.5) y cast a int:")
a_fill.groupBy(F.col("PP").cast("int").alias("PP_int")).count().show()
print("NOM_PELIGRO tras fillna('Sismo/Huaico'):")
a_fill.groupBy("NOM_PELIGRO").count().show(truncate=False)

titulo("0.7 EXPLICACION")
print("""* Distritos de Lima: el analisis no filtraba por EPS ni por departamento. SUNASS es un
  reporte NACIONAL y SEDAPAL (Lima) concentra la mayoria de filas (ver 0.2), por eso el
  top 5 eran distritos de Lima. SEDALIB / La Libertad es solo una fraccion pequena.
* PP = 12 mm constante: SENAMHI solo trae estaciones en distritos de Arequipa, Lima-provincias
  y Tacna (ver 0.3) y ninguno coincide con un distrito de interrupciones (0.4, coincidencia
  0 %). El JOIN por DISTRITO no encontro clima para NINGUNA fila (0.5-A), todo PP quedo
  nulo y fillna('12.5') lo relleno; al castear a int quedo 12. No es un dato observado.
* 100 % 'Sismo/Huaico': INGEMMET escribe NM_DIST en Tipo Titulo ('La Molina') y SUNASS en
  MAYUSCULAS ('LA MOLINA'); sin normalizar no hubo ninguna coincidencia (0.5-A) y
  fillna('Sismo/Huaico') relleno el 100 %. Ademas 'Sismo/Huaico' no es una categoria
  de INGEMMET. Al normalizar (version B) el JOIN 1-a-N multiplica filas.
* En la version B la fecha se parseaba con 'yyyy-MM-dd' sobre valores 'yyyyMMdd', por lo
  que FECHA_CORTE era nula y el JOIN de clima por fecha tampoco podia coincidir.""")

spark.stop()
