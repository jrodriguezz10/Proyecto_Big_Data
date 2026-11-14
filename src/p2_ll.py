"""02c. Pregunta 2 (lluvia vs interrupciones) solo La Libertad, 2019-06..2024-05. Analisis descriptivo."""
import sys; sys.path.insert(0, "/app/src")
import comun as C
from comun import titulo
from pyspark.sql import functions as F

spark = C.spark_session("SEDALIB_P2_LL")
T = F.col("ANIO") * 12 + F.col("MES") - 1
d = (spark.read.parquet(C.PROCESSED + "master_panel_clima.parquet")
     .filter((F.col("DEPTO_N") == "LA LIBERTAD") & (T >= 2019 * 12 + 5) & (T <= 2024 * 12 + 4))
     .withColumn("y", F.col("TARGET").cast("int"))).cache()

def tabla(df, col, orden=None):
    rows = (df.groupBy(col).agg(F.count("*").alias("n"), F.sum("y").alias("k"))
            .orderBy(orden if orden is not None else col).collect())
    print("%-14s %14s %16s %8s" % (col, "distrito-meses", "con_interrupcion", "tasa_%"))
    for r in rows:
        print("%-14s %14d %16d %8.1f" % (r[0], r["n"], r["k"], 100.0 * r["k"] / r["n"]))
    tot = df.agg(F.count("*"), F.sum("y")).first()
    print("%-14s %14d %16d %8.1f" % ("TOTAL", tot[0], tot[1], 100.0 * tot[1] / tot[0]))
    return rows

titulo("02c.0 POBLACION")
r = d.agg(F.count("*").alias("n"), F.countDistinct("DIST_N").alias("dist"), F.sum("y").alias("k"),
          F.min(T).alias("a"), F.max(T).alias("b"), F.sum(F.col("PP_ANOMALIA").isNull().cast("int")).alias("nul"),
          F.countDistinct("CLIMA_FUENTE").alias("f")).first()
print("La Libertad: %d distrito-meses | %d distritos | %d-%02d a %d-%02d | con interrupcion=%d (%.1f %%)"
      % (r["n"], r["dist"], r["a"] // 12, r["a"] % 12 + 1, r["b"] // 12, r["b"] % 12 + 1, r["k"], 100.0 * r["k"] / r["n"]))
print("Fuente de clima: %s | PP_ANOMALIA nula: %d"
      % ([x[0] for x in d.select("CLIMA_FUENTE").distinct().collect()], r["nul"]))
print("Interrupcion = TARGET (>= 1 interrupcion en el distrito-mes, SUNASS). Clima del MISMO mes (sin rezago).")
print("PP_ANOMALIA = PP del mes / promedio del mismo distrito y mes calendario (NASA POWER). MES_LLUVIOSO = PP_ANOMALIA > 1.5.")

titulo("02c.1 MES_LLUVIOSO -> tasa de interrupcion")
tabla(d.withColumn("MES_LLUVIOSO", F.coalesce(F.col("MES_LLUVIOSO").cast("string"), F.lit("sin dato"))), "MES_LLUVIOSO")

titulo("02c.2 Tramos de PP_ANOMALIA -> tasa de interrupcion")
print("Tramos: <0.5 | [0.5, 1) | [1, 1.5] | (1.5, 3] | >3  (1.5 es el corte de MES_LLUVIOSO)")
a = F.col("PP_ANOMALIA")
d2 = d.withColumn("TRAMO", F.when(a.isNull(), "9 sin dato").when(a < 0.5, "1 <0.5").when(a < 1, "2 0.5-1")
                  .when(a <= 1.5, "3 1-1.5").when(a <= 3, "4 1.5-3").otherwise("5 >3"))
rows = tabla(d2, "TRAMO")
with open("/tmp/p2_tramos.csv", "w") as f:
    f.write("tramo,n,k\n")
    for x in rows:
        f.write("%s,%d,%d\n" % (x[0], x["n"], x["k"]))

titulo("02c.3 Ciclon Yaku: enero-abril 2023 vs enero-abril de los otros anios")
e = d.filter(F.col("MES") <= 4)
rows = (e.groupBy("ANIO").agg(F.count("*").alias("n"), F.sum("y").alias("k"),
                              F.avg("PP_ANOMALIA").alias("anom"), F.sum("MES_LLUVIOSO").alias("ll"))
        .orderBy("ANIO").collect())
print("%-6s %14s %16s %8s %16s %14s" % ("anio", "distrito-meses", "con_interrupcion", "tasa_%",
                                        "PP_ANOMALIA_med", "MES_LLUVIOSO=1"))
for x in rows:
    print("%-6d %14d %16d %8.1f %16.2f %14d" % (x["ANIO"], x["n"], x["k"], 100.0 * x["k"] / x["n"], x["anom"], x["ll"]))
y23 = [x for x in rows if x["ANIO"] == 2023][0]
otr = [x for x in rows if x["ANIO"] != 2023]
n_o, k_o = sum(x["n"] for x in otr), sum(x["k"] for x in otr)
print("\nene-abr 2023        : %d/%d = %.1f %%" % (y23["k"], y23["n"], 100.0 * y23["k"] / y23["n"]))
print("ene-abr otros anios : %d/%d = %.1f %% (%s)" % (k_o, n_o, 100.0 * k_o / n_o, ", ".join(str(x["ANIO"]) for x in otr)))
print("Diferencia          : %+.1f puntos porcentuales" % (100.0 * y23["k"] / y23["n"] - 100.0 * k_o / n_o))
print("Nota: 2019 no entra (el periodo empieza en 2019-06). Descriptivo, sin prueba de significancia;")
print("      13 distritos x 4 meses = 52 distrito-meses por anio.")
spark.stop()
