"""6. EDA (La Libertad) y modelo base sobre el panel distrito-mes.

Ejecutar (despues de procesamiento_spark.py):
    docker exec spark_node /spark/bin/spark-submit /app/src/analisis_modelo.py
Escribe las cifras de cada grafico en /app/evidencias/eda_g*.csv (los dibuja graficos_locales.py).

Sin fuga de datos: solo variables rezagadas (mes anterior) o estaticas del distrito, mas el
mes calendario. Split temporal: ultimo anio del panel (2024) = prueba.
Dos variantes:
  M1 todos los distritos (sin clima)  -> incluye La Libertad, se usa para el ranking de riesgo
  M2 solo departamentos con estacion SENAMHI (LIMA, AREQUIPA, TACNA) + clima rezagado
"""
import csv

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

import comun as C
from comun import titulo

spark = C.spark_session("SEDALIB_AnalisisModelo")
panel = spark.read.parquet(C.PANEL).cache()
LL = "LA LIBERTAD"
EVID = "/app/evidencias/"


def guardar_csv(df, nombre):
    filas = df.collect()
    with open(EVID + nombre, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows([list(r) for r in filas])


def tasa(df, *grupo):
    return (df.groupBy(*grupo).agg(F.count("*").alias("distrito_meses"), F.sum("TARGET").alias("meses_con_int"),
                                   F.round(F.avg("TARGET") * 100, 2).alias("tasa_int_pct"),
                                   F.sum("N_INTERRUPCIONES").alias("interrupciones")))


# =====================================================================  EDA
titulo("4.1 EDA - LA LIBERTAD (SEDALIB)")
ll = panel.filter(F.col("DEPTO_N") == LL)
r = ll.agg(F.countDistinct("DIST_N").alias("d"), F.count("*").alias("n"), F.sum("N_INTERRUPCIONES").alias("i"),
           F.sum("CLIMA_DISPONIBLE").alias("c")).first()
print("La Libertad en el panel: %d distritos, %d distrito-meses, %d interrupciones, %d filas con clima"
      % (r["d"], r["n"], r["i"], r["c"]))

print("\n[g1] Top distritos de La Libertad por interrupciones (2019-06 a 2024-05):")
g1 = (ll.groupBy("DIST_N").agg(F.sum("N_INTERRUPCIONES").alias("interrupciones"),
                               F.sum("N_IMPREVISTAS").alias("imprevistas"),
                               F.sum("TARGET").alias("meses_con_int"),
                               F.round(F.avg("TARGET") * 100, 2).alias("tasa_int_pct"))
      .orderBy(F.desc("interrupciones"), "DIST_N"))
g1.show(20, truncate=False)
guardar_csv(g1, "eda_g1_distritos_ll.csv")

print("[g2] Tasa de interrupcion por rango de PP_TOTAL mensual")
print("     AVISO: SENAMHI no tiene estaciones en La Libertad -> se usa el nivel NACIONAL disponible")
print("     (distritos de LIMA, AREQUIPA y TACNA, con PP promedio de las estaciones del departamento).")
clima = panel.filter(F.col("CLIMA_DISPONIBLE") == 1)
rango = (F.when(F.col("PP_TOTAL") == 0, "0 mm").when(F.col("PP_TOTAL") <= 10, "0-10 mm")
         .when(F.col("PP_TOTAL") <= 25, "10-25 mm").when(F.col("PP_TOTAL") <= 50, "25-50 mm")
         .when(F.col("PP_TOTAL") <= 100, "50-100 mm").otherwise(">100 mm"))
orden = (F.when(F.col("PP_TOTAL") == 0, 0).when(F.col("PP_TOTAL") <= 10, 1).when(F.col("PP_TOTAL") <= 25, 2)
         .when(F.col("PP_TOTAL") <= 50, 3).when(F.col("PP_TOTAL") <= 100, 4).otherwise(5))
g2 = tasa(clima.withColumn("RANGO_PP", rango).withColumn("ORDEN", orden), "ORDEN", "RANGO_PP").orderBy("ORDEN")
g2.show(truncate=False)
guardar_csv(g2, "eda_g2_tasa_por_pp.csv")
print("     Mismo cruce con PP del mes ANTERIOR (PP_TOTAL_LAG1):")
g2l = tasa(clima.filter(F.col("PP_TOTAL_LAG1").isNotNull()).withColumn("PP_TOTAL", F.col("PP_TOTAL_LAG1"))
           .withColumn("RANGO_PP", rango).withColumn("ORDEN", orden), "ORDEN", "RANGO_PP").orderBy("ORDEN")
g2l.show(truncate=False)

print("[g3] Tasa de interrupcion por cantidad de peligros inventariados (INGEMMET) en el distrito")
print("     AVISO: La Libertad solo tiene 13 distritos -> se muestra NACIONAL con La Libertad resaltada.")
bins = (F.when(F.col("N_PELIGROS") == 0, "0").when(F.col("N_PELIGROS") <= 5, "1-5")
        .when(F.col("N_PELIGROS") <= 20, "6-20").when(F.col("N_PELIGROS") <= 50, "21-50").otherwise(">50"))
ordb = (F.when(F.col("N_PELIGROS") == 0, 0).when(F.col("N_PELIGROS") <= 5, 1).when(F.col("N_PELIGROS") <= 20, 2)
        .when(F.col("N_PELIGROS") <= 50, 3).otherwise(4))
pb = panel.withColumn("RANGO_PELIGROS", bins).withColumn("ORDEN", ordb) \
    .withColumn("AMBITO", F.when(F.col("DEPTO_N") == LL, "LA LIBERTAD").otherwise("RESTO DEL PAIS"))
g3 = tasa(pb, "AMBITO", "ORDEN", "RANGO_PELIGROS") \
    .join(pb.groupBy("AMBITO", "ORDEN").agg(F.countDistinct("DEPTO_N", "DIST_N").alias("distritos")), ["AMBITO", "ORDEN"]) \
    .orderBy("AMBITO", "ORDEN")
g3.show(truncate=False)
guardar_csv(g3, "eda_g3_tasa_por_peligros.csv")
print("Distritos de La Libertad y sus peligros:")
ll.select("DIST_N", "N_PELIGROS", "TIPO_PELIGRO_PRINCIPAL", "PCT_SIST_MAS_20_ANIOS").distinct() \
    .orderBy(F.desc("N_PELIGROS")).show(truncate=False)

# =====================================================================  MODELO
titulo("4.2 MODELO BASE (MLlib) - split temporal")
ESTATICAS = ["N_PELIGROS", "PCT_SIST_MAS_20_ANIOS", "PCT_SIST_NO_OPERA", "INFRA_DISPONIBLE"]
REZAGOS = ["INT_LAG1", "TARGET_LAG1", "INT_PREV3"]
CLIMA_LAG = ["PP_TOTAL_LAG1", "PP_MAX_LAG1", "DIAS_LLUVIA_ALTA_LAG1"]
anio_test = panel.agg(F.max("ANIO")).first()[0]
base = panel.filter(F.col("INT_LAG1").isNotNull()).withColumn("label", F.col("TARGET").cast("double"))
for c in ESTATICAS + REZAGOS + CLIMA_LAG:
    base = base.withColumn(c, F.col(c).cast("double"))
print("Prueba = ANIO %d (ene-may, fin del archivo) | Entrenamiento = ANIOs anteriores" % anio_test)
print("Variables: rezagadas %s + estaticas %s + MES (one-hot)" % (REZAGOS, ESTATICAS))
print("Excluidas por fuga: N_INTERRUPCIONES, N_IMPREVISTAS, PP_TOTAL/PP_MAX del mismo mes, TARGET.")

ev_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
ev_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")


def evaluar(pred, col_score, nombre):
    auc = ev_roc.setRawPredictionCol(col_score).evaluate(pred)
    aupr = ev_pr.setRawPredictionCol(col_score).evaluate(pred)
    p = pred.withColumn("p1", vector_to_array(col_score)[1] if col_score == "probability" else F.col(col_score))
    cm = p.groupBy((F.col("p1") >= 0.5).cast("int").alias("pred"), F.col("label").cast("int").alias("real")).count().collect()
    m = {(r["real"], r["pred"]): r["count"] for r in cm}
    tn, fp, fn, tp = m.get((0, 0), 0), m.get((0, 1), 0), m.get((1, 0), 0), m.get((1, 1), 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    print("   %-34s AUC-ROC=%.4f  AUC-PR=%.4f | TN=%5d FP=%5d FN=%5d TP=%5d | precision=%.3f recall=%.3f"
          % (nombre, auc, aupr, tn, fp, fn, tp, prec, rec))
    return {"modelo": nombre, "auc_roc": auc, "auc_pr": aupr, "tn": tn, "fp": fp, "fn": fn, "tp": tp}


def experimento(nombre, datos, numericas):
    titulo("Variante %s" % nombre)
    train = datos.filter(F.col("ANIO") < anio_test).cache()
    test = datos.filter(F.col("ANIO") == anio_test).cache()
    nt, ns = train.count(), test.count()
    pt = train.agg(F.avg("label")).first()[0]
    ps = test.agg(F.avg("label")).first()[0]
    print("train=%d filas (prevalencia %.4f) | test=%d filas (prevalencia %.4f) | distritos=%d"
          % (nt, pt, ns, ps, datos.select("DEPTO_N", "DIST_N").distinct().count()))
    imp_cols = [c for c in numericas if c in ("PCT_SIST_MAS_20_ANIOS", "PCT_SIST_NO_OPERA") or c in CLIMA_LAG]
    otras = [c for c in numericas if c not in imp_cols]
    etapas = [Imputer(strategy="median", inputCols=imp_cols, outputCols=[c + "_imp" for c in imp_cols]),
              OneHotEncoder(inputCols=["MES"], outputCols=["MES_oh"], dropLast=True),
              VectorAssembler(inputCols=otras + [c + "_imp" for c in imp_cols] + ["MES_oh"], outputCol="features")]
    nombres = otras + imp_cols + ["MES_%d" % m for m in range(1, 12)]
    lr = Pipeline(stages=etapas + [StandardScaler(inputCol="features", outputCol="features_std"),
                                   LogisticRegression(featuresCol="features_std", labelCol="label", maxIter=100)]).fit(train)
    dt = Pipeline(stages=etapas + [DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=5,
                                                          minInstancesPerNode=20, seed=42)]).fit(train)
    # linea base ingenua: tasa historica del distrito en entrenamiento (tasa global si es distrito nuevo)
    hist = train.groupBy("DEPTO_N", "DIST_N").agg(F.avg("label").alias("TASA_HIST"))
    ingenua = test.join(hist, ["DEPTO_N", "DIST_N"], "left").fillna({"TASA_HIST": pt})
    print("\nMetricas en prueba (umbral 0.5 para la matriz de confusion):")
    res = [evaluar(lr.transform(test), "probability", "Regresion Logistica"),
           evaluar(dt.transform(test), "probability", "Arbol de Decision"),
           evaluar(ingenua, "TASA_HIST", "Linea base ingenua (tasa hist.)")]
    print("\nImportancia - Arbol de Decision (featureImportances):")
    imp = dt.stages[-1].featureImportances.toArray()
    for n, v in sorted(zip(nombres, imp), key=lambda x: -x[1])[:10]:
        print("   %-24s %.4f" % (n, v))
    print("Coeficientes estandarizados - Regresion Logistica (top 10 por |coef|):")
    coef = lr.stages[-1].coefficients.toArray()
    for n, v in sorted(zip(nombres, coef), key=lambda x: -abs(x[1]))[:10]:
        print("   %-24s %+.4f" % (n, v))
    return lr, dt, res, test, ingenua


lr1, dt1, res1, test1, ing1 = experimento("M1 - todos los distritos (sin clima)", base, REZAGOS + ESTATICAS)
lr2, dt2, res2, _, _ = experimento("M2 - distritos con estacion SENAMHI (+ clima rezagado)",
                                base.filter(F.col("CLIMA_DISPONIBLE") == 1), REZAGOS + ESTATICAS + CLIMA_LAG)

# probabilidades de prueba de M1 (para las curvas ROC/PR de graficos_resultados.py)
CLAVE = ["DEPTO_N", "DIST_N", "ANIO", "MES"]
pred = (lr1.transform(test1).select(*CLAVE, "label", vector_to_array("probability")[1].alias("P_LR"))
        .join(dt1.transform(test1).select(*CLAVE, vector_to_array("probability")[1].alias("P_ARBOL")), CLAVE)
        .join(ing1.select(*CLAVE, F.col("TASA_HIST").alias("P_INGENUA")), CLAVE)
        .orderBy(*CLAVE))
guardar_csv(pred, "modelo_m1_predicciones_test.csv")
print("\nPredicciones de prueba M1 guardadas: %d filas -> evidencias/modelo_m1_predicciones_test.csv" % pred.count())

titulo("TOP 10 DISTRITOS DE LA LIBERTAD CON MAS RIESGO (modelo M1)")
mejor = max(res1[:2], key=lambda r: r["auc_pr"])["modelo"]
modelo = lr1 if mejor == "Regresion Logistica" else dt1
print("Modelo usado: %s (mayor AUC-PR en M1). Riesgo = probabilidad media predicha en %d." % (mejor, anio_test))
top = (modelo.transform(test1.filter(F.col("DEPTO_N") == LL))
       .withColumn("p1", vector_to_array("probability")[1])
       .groupBy("DIST_N").agg(F.round(F.avg("p1"), 4).alias("riesgo_medio"), F.round(F.max("p1"), 4).alias("riesgo_max"),
                              F.sum("label").cast("int").alias("meses_con_int_real"),
                              F.count("*").alias("meses_evaluados"))
       .orderBy(F.desc("riesgo_medio")))
top.show(10, truncate=False)
guardar_csv(top, "modelo_top_riesgo_ll.csv")
spark.stop()
