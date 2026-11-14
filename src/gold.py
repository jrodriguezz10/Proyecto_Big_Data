"""5. Capa GOLD: aplica M1-GBT (umbral fijo 0.1084) al panel distrito-mes -> curated/gold/ (Parquet).

Mismo codigo en Docker y en EMR Serverless (rutas por variables de entorno, ver comun.py):
    AWS  : aws/05_gold_athena.py --gold   (SEDALIB_DATALAKE=s3://<bucket>/)
    local: docker exec spark_node /spark/bin/spark-submit --py-files /app/src/comun.py /app/src/gold.py

Entrada : processed/master_panel_clima.parquet, raw/operaciones/sunass_interrupciones.csv (provincia),
          raw/referencia/ubigeo_inei_2022.csv (INEI, UBIGEO 2022 - 1891 distritos)
Salida  : curated/gold/{gold_riesgo_distrito_mes, dim_distrito, dim_tiempo, gold_kpi_mensual,
          gold_comparacion_modelos}/  (un archivo Parquet por tabla, sin particiones)

Decisiones:
* M1-GBT identico a src/modelo_definitivo.py (mismas variables, Imputer, OneHot, StandardScaler,
  GBT maxIter=50 maxDepth=4 stepSize=0.1 seed=42). Se copia la definicion porque ese script se ejecuta
  entero al importarse; su logica no cambia.
* Probabilidad FUERA DE MUESTRA: cada mes se predice con un modelo entrenado solo con meses anteriores
  (4 cortes semestrales 2022-2023 + holdout 2024-01..05 = 5 entrenamientos). Los meses 2019-2021 no
  tienen prediccion fuera de muestra y no entran en la tabla de hechos.
* Umbral fijo 0.1084 (elegido en validacion agrupada en la corrida AWS del paso 3a). alerta = prob >= umbral.
* Control: AUC-PR nacional del holdout debe ser 0.7248 (paso 3a en AWS); si difiere > 0.001 se detiene.
* gold_comparacion_modelos (holdout 2024): M1-GBT vs tasa historica (mean(TARGET) del distrito en
  entrenamiento, umbral 0.1111 de evidencias/04b_tasa_historica.txt) vs regla ingenua (TARGET_LAG1, umbral 1).
* Sin datos personales: el panel ya esta agregado por distrito-mes (no hay IDs ni nombres de personas).
"""
import os

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window, functions as F

import comun as C
from comun import norm, titulo

PANEL_CLIMA = C.PROCESSED + "master_panel_clima.parquet"
GOLD = C.DATALAKE + "curated/gold/"
UBIGEO_CSV = C.RAW + "referencia/ubigeo_inei_2022.csv"
LL = "LA LIBERTAD"
SEED = 42
UMBRAL_GBT, UMBRAL_TASA, UMBRAL_INGENUA = 0.1084, 0.1111, 1.0
CONTROL_AUC_PR, TOL = 0.7248, 0.001
CLAVE = ["DEPTO_N", "DIST_N", "ANIO", "MES"]
CATEG = ["MES_STR", "DEPTO_N", "TIPO_PELIGRO"]
NUM_M1 = ["INT_LAG1", "TARGET_LAG1", "INT_PREV3", "N_PELIGROS", "PCT_SIST_MAS_20_ANIOS",
          "PCT_SIST_NO_OPERA", "INFRA_DISPONIBLE"]
IMPUTAR = ["PCT_SIST_MAS_20_ANIOS", "PCT_SIST_NO_OPERA"]


def t(anio, mes):
    return anio * 12 + mes - 1


CORTES = [("cv_2022-S1", t(2022, 1), t(2022, 6)), ("cv_2022-S2", t(2022, 7), t(2022, 12)),
          ("cv_2023-S1", t(2023, 1), t(2023, 6)), ("cv_2023-S2", t(2023, 7), t(2023, 12)),
          ("holdout_2024", t(2024, 1), t(2024, 5))]


def pipeline_m1_gbt():
    otras = [c for c in NUM_M1 if c not in IMPUTAR]
    return Pipeline(stages=[
        StringIndexer(inputCols=CATEG, outputCols=[c + "_idx" for c in CATEG], handleInvalid="keep"),
        OneHotEncoder(inputCols=[c + "_idx" for c in CATEG], outputCols=[c + "_oh" for c in CATEG],
                      handleInvalid="keep"),
        Imputer(strategy="median", inputCols=IMPUTAR, outputCols=[c + "_imp" for c in IMPUTAR]),
        VectorAssembler(inputCols=otras + [c + "_imp" for c in IMPUTAR] + [c + "_oh" for c in CATEG],
                        outputCol="features"),
        StandardScaler(inputCol="features", outputCol="features_std"),
        GBTClassifier(maxIter=50, maxDepth=4, stepSize=0.1, seed=SEED, featuresCol="features_std", labelCol="label")])


# ---------------------------------------------------------------- metricas (mismas que modelo_definitivo.py)
def _curva(y, s):
    o = np.argsort(-s, kind="mergesort")
    y, s = y[o], s[o]
    tp, fp = np.cumsum(y), np.cumsum(1 - y)
    idx = np.r_[np.where(np.diff(s) != 0)[0], len(s) - 1]
    return s[idx], tp[idx], fp[idx]


def auc_pr(y, s):
    if y.sum() == 0:
        return float("nan")
    _, tp, fp = _curva(y, s)
    rec, prec = tp / y.sum(), tp / (tp + fp)
    return float(np.trapz(np.r_[prec[0], prec], np.r_[0.0, rec]))


def auc_roc(y, s):
    P, N = y.sum(), len(y) - y.sum()
    if P == 0 or N == 0:
        return float("nan")
    _, tp, fp = _curva(y, s)
    return float(np.trapz(np.r_[0.0, tp / P, 1.0], np.r_[0.0, fp / N, 1.0]))


def matriz(y, s, u):
    p = (s >= u).astype(int)
    tp = int(((p == 1) & (y == 1)).sum()); fp = int(((p == 1) & (y == 0)).sum())
    fn = int(((p == 0) & (y == 1)).sum()); tn = int(((p == 0) & (y == 0)).sum())
    return dict(tp=tp, fp=fp, fn=fn, tn=tn, precision=tp / (tp + fp) if tp + fp else None,
                recall=tp / (tp + fn) if tp + fn else None)


# ---------------------------------------------------------------- datos (igual que modelo_definitivo.py)
spark = C.spark_session("SEDALIB_Gold", **{"spark.sql.shuffle.partitions": "16"})
panel = spark.read.parquet(PANEL_CLIMA)
datos = (panel.filter(F.col("INT_LAG1").isNotNull())
         .withColumn("label", F.col("TARGET").cast("double"))
         .withColumn("T", F.col("ANIO") * 12 + F.col("MES") - 1)
         .withColumn("MES_STR", F.format_string("%02d", F.col("MES")))
         .withColumn("TIPO_PELIGRO", F.coalesce(F.col("TIPO_PELIGRO_PRINCIPAL"), F.lit("SIN_DATO"))))
for c in NUM_M1:
    datos = datos.withColumn(c, F.col(c).cast("double"))
datos = datos.select(*CLAVE, "T", "label", *CATEG, *sorted(set(NUM_M1))).cache()

titulo("5.1 PREDICCION FUERA DE MUESTRA M1-GBT (5 entrenamientos, umbral fijo %.4f)" % UMBRAL_GBT)
partes, holdout = [], None
for nombre, a, b in CORTES:
    tr = datos.filter(F.col("T") < a).cache()
    va = datos.filter((F.col("T") >= a) & (F.col("T") <= b)).cache()
    pred = (pipeline_m1_gbt().fit(tr).transform(va)
            .select(*CLAVE, "label", "TARGET_LAG1", vector_to_array("probability")[1].alias("PROB"))
            .withColumn("CONJUNTO", F.lit(nombre)))
    tasa = tr.groupBy("DEPTO_N", "DIST_N").agg(F.avg("label").alias("TASA_HIST"))
    prev = tr.agg(F.avg("label")).first()[0]
    pred = (pred.join(tasa, ["DEPTO_N", "DIST_N"], "left")
            .withColumn("TASA_HIST", F.coalesce("TASA_HIST", F.lit(prev))).cache())
    r = pred.agg(F.count("*").alias("n"), F.sum("label").alias("pos"),
                 F.sum((F.col("PROB") >= UMBRAL_GBT).cast("int")).alias("alertas")).first()
    print("%-13s entrena T<%d-%02d (n=%d) | predice %d-%02d..%d-%02d n=%d positivos=%d alertas=%d"
          % (nombre, a // 12, a % 12 + 1, tr.count(), a // 12, a % 12 + 1, b // 12, b % 12 + 1,
             r["n"], int(r["pos"]), r["alertas"]))
    partes.append(pred)
    if nombre == "holdout_2024":
        holdout = pred.orderBy(*CLAVE).collect()
    tr.unpersist(); va.unpersist()

pred = partes[0]
for p in partes[1:]:
    pred = pred.unionByName(p)

# control contra la corrida AWS del paso 3a
y = np.array([int(x["label"]) for x in holdout])
s_gbt = np.array([x["PROB"] for x in holdout])
ap = auc_pr(y, s_gbt)
print("\nControl holdout 2024 nacional: AUC-PR M1-GBT = %.4f (esperado %.4f, tolerancia %.3f) -> %s"
      % (ap, CONTROL_AUC_PR, TOL, "OK" if abs(ap - CONTROL_AUC_PR) <= TOL else "DIFERENTE"))
assert abs(ap - CONTROL_AUC_PR) <= TOL, "El modelo no reproduce el AUC-PR del paso 3a; no se escribe gold"

# ---------------------------------------------------------------- dim_distrito
titulo("5.2 DIMENSIONES")
inte = (C.leer_interrupciones(spark).select(norm(F.col("DEPARTAMENTO")).alias("DEPTO_N"),
                                            norm(F.col("DISTRITO")).alias("DIST_N"),
                                            norm(F.col("PROVINCIA")).alias("PROV_N"))
        .filter(F.col("DIST_N").isNotNull() & F.col("PROV_N").isNotNull()))
prov = (inte.groupBy("DEPTO_N", "DIST_N", "PROV_N").count()
        .withColumn("rk", F.row_number().over(Window.partitionBy("DEPTO_N", "DIST_N")
                                              .orderBy(F.desc("count"), "PROV_N")))
        .filter("rk = 1").select("DEPTO_N", "DIST_N", "PROV_N"))
multi = inte.groupBy("DEPTO_N", "DIST_N").agg(F.countDistinct("PROV_N").alias("np")).filter("np > 1").count()

ubi = (spark.read.csv(UBIGEO_CSV, header=True, sep=";", encoding="ISO-8859-1")
       .select(F.col("IDDIST").alias("UBIGEO"), norm(F.col("NOMBDEP")).alias("DEPTO_N"),
               norm(F.col("NOMBPROV")).alias("PROV_N"), norm(F.col("NOMBDIST")).alias("DIST_N"))
       .filter(F.col("UBIGEO").isNotNull()))
n_ubi = ubi.count()
ubi_unico = (ubi.groupBy("DEPTO_N", "DIST_N").agg(F.count("*").alias("k"), F.first("UBIGEO").alias("UBIGEO_DX"))
             .filter("k = 1").drop("k"))

dist = panel.groupBy("DEPTO_N", "DIST_N").agg(F.max("N_PELIGROS").alias("n_peligros"),
                                              F.round(F.avg("CLIMA_DISPONIBLE"), 4).alias("cobertura_clima"))
dim_distrito = (dist.join(prov, ["DEPTO_N", "DIST_N"], "left")
                .join(ubi.withColumnRenamed("UBIGEO", "UBIGEO_DPX"), ["DEPTO_N", "PROV_N", "DIST_N"], "left")
                .join(ubi_unico, ["DEPTO_N", "DIST_N"], "left")
                .withColumn("METODO_UBIGEO", F.when(F.col("UBIGEO_DPX").isNotNull(), "depto+prov+dist")
                            .when(F.col("UBIGEO_DX").isNotNull(), "depto+dist (unico)"))
                .select(F.coalesce("UBIGEO_DPX", "UBIGEO_DX").alias("ubigeo"),
                        F.col("DEPTO_N").alias("departamento"), F.col("PROV_N").alias("provincia"),
                        F.col("DIST_N").alias("distrito"), F.col("n_peligros").cast("int").alias("n_peligros"),
                        "cobertura_clima", "METODO_UBIGEO")
                .cache())
n_dist = dim_distrito.count()
assert n_dist == dist.count(), "explosion de filas en dim_distrito"
print("INEI UBIGEO 2022: %d distritos | panel: %d distritos | con >1 provincia en SUNASS: %d (se toma la mas frecuente)"
      % (n_ubi, n_dist, multi))
for r in dim_distrito.groupBy("METODO_UBIGEO").count().orderBy("METODO_UBIGEO").collect():
    print("   ubigeo por %-20s: %d" % (r["METODO_UBIGEO"] or "SIN UBIGEO", r["count"]))
print("Distritos SIN ubigeo:")
for r in dim_distrito.filter(F.col("ubigeo").isNull()).orderBy("departamento", "distrito").collect():
    print("   %s / %s / %s" % (r["departamento"], r["provincia"], r["distrito"]))

# ---------------------------------------------------------------- hechos
hechos = (pred.join(dim_distrito.select(F.col("departamento").alias("DEPTO_N"), F.col("distrito").alias("DIST_N"),
                                        "ubigeo"), ["DEPTO_N", "DIST_N"], "left")
          .select("ubigeo", F.col("DEPTO_N").alias("departamento"), F.col("DIST_N").alias("distrito"),
                  F.col("ANIO").cast("int").alias("anio"), F.col("MES").cast("int").alias("mes"),
                  F.round("PROB", 6).alias("prob_interrupcion"),
                  (F.col("PROB") >= UMBRAL_GBT).cast("int").alias("alerta"),
                  F.col("label").cast("int").alias("interrupcion_real"))
          .orderBy("departamento", "distrito", "anio", "mes").cache())

dim_tiempo = (hechos.select("anio", "mes").distinct()
              .withColumn("trimestre", ((F.col("mes") - 1) / 3 + 1).cast("int"))
              .withColumn("temporada_lluvia", F.col("mes").isin(12, 1, 2, 3, 4).cast("int"))
              .orderBy("anio", "mes"))


# ---------------------------------------------------------------- KPI mensual
def kpi(df, grupo):
    return df.groupBy("anio", "mes", *grupo).agg(
        F.count("*").alias("n_distritos"),
        F.sum("interrupcion_real").alias("distritos_con_interrupcion"),
        F.round(F.avg("interrupcion_real"), 4).alias("tasa_interrupcion"),
        F.sum("alerta").alias("distritos_en_alerta"),
        F.sum(((F.col("alerta") == 1) & (F.col("interrupcion_real") == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("alerta") == 1) & (F.col("interrupcion_real") == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("alerta") == 0) & (F.col("interrupcion_real") == 1)).cast("int")).alias("fn"))


kpi_dep = kpi(hechos, ["departamento"])
kpi_nac = kpi(hechos, []).withColumn("departamento", F.lit("NACIONAL"))
gold_kpi = (kpi_dep.unionByName(kpi_nac)
            .withColumn("precision_alertas", F.round(F.col("tp") / F.when(F.col("tp") + F.col("fp") > 0,
                                                                           F.col("tp") + F.col("fp")), 4))
            .withColumn("recall_alertas", F.round(F.col("tp") / F.when(F.col("tp") + F.col("fn") > 0,
                                                                        F.col("tp") + F.col("fn")), 4))
            .withColumn("es_la_libertad", (F.col("departamento") == LL).cast("int"))
            .select("anio", "mes", "departamento", "es_la_libertad", "n_distritos", "distritos_con_interrupcion",
                    "tasa_interrupcion", "distritos_en_alerta", "tp", "fp", "fn", "precision_alertas",
                    "recall_alertas")
            .orderBy("anio", "mes", "departamento"))

# ---------------------------------------------------------------- comparacion de modelos (holdout 2024)
titulo("5.3 COMPARACION DE MODELOS - HOLDOUT 2024-01..05")
es_ll = np.array([x["DEPTO_N"] == LL for x in holdout])
scores = {"M1-GBT": (s_gbt, UMBRAL_GBT),
          "Tasa historica": (np.array([x["TASA_HIST"] for x in holdout]), UMBRAL_TASA),
          "Regla ingenua": (np.array([float(x["TARGET_LAG1"]) for x in holdout]), UMBRAL_INGENUA)}
filas = []
print("%-12s %-15s %5s %4s %7s %7s %7s %7s %7s %5s %5s %5s %5s"
      % ("ambito", "modelo", "n", "pos", "AUC-PR", "AUC-ROC", "umbral", "prec", "recall", "TP", "FP", "FN", "TN"))
for ambito, mask in (("NACIONAL", np.ones(len(y), bool)), (LL, es_ll)):
    for modelo, (s, u) in scores.items():
        m = matriz(y[mask], s[mask], u)
        f = dict(conjunto="holdout_2024", ambito=ambito, modelo=modelo, n=int(mask.sum()),
                 positivos=int(y[mask].sum()), auc_pr=round(auc_pr(y[mask], s[mask]), 4),
                 auc_roc=round(auc_roc(y[mask], s[mask]), 4), umbral=u,
                 precision=round(m["precision"] or 0.0, 4), recall=round(m["recall"] or 0.0, 4),
                 tp=m["tp"], fp=m["fp"], fn=m["fn"], tn=m["tn"])
        filas.append(f)
        print("%-12s %-15s %5d %4d %7.4f %7.4f %7.4f %7.4f %7.4f %5d %5d %5d %5d"
              % (ambito, modelo, f["n"], f["positivos"], f["auc_pr"], f["auc_roc"], u, f["precision"],
                 f["recall"], f["tp"], f["fp"], f["fn"], f["tn"]))
comparacion = spark.createDataFrame(filas).select("conjunto", "ambito", "modelo", "n", "positivos", "auc_pr",
                                                  "auc_roc", "umbral", "precision", "recall", "tp", "fp", "fn", "tn")

# ---------------------------------------------------------------- escritura
titulo("5.4 ESCRITURA EN curated/gold/ (Parquet, 1 archivo por tabla)")
tablas = [("gold_riesgo_distrito_mes", hechos), ("dim_distrito", dim_distrito.drop("METODO_UBIGEO")),
          ("dim_tiempo", dim_tiempo), ("gold_kpi_mensual", gold_kpi), ("gold_comparacion_modelos", comparacion)]
for nombre, df in tablas:
    df.coalesce(1).write.mode("overwrite").parquet(GOLD + nombre)
    leido = spark.read.parquet(GOLD + nombre)
    print("%-26s %6d filas | %s" % (nombre, leido.count(), ", ".join("%s:%s" % c for c in leido.dtypes)))

titulo("5.5 RESUMEN")
r = hechos.agg(F.count("*").alias("n"), F.sum("alerta").alias("al"), F.sum("interrupcion_real").alias("re"),
               F.min(F.col("anio") * 100 + F.col("mes")).alias("a"),
               F.max(F.col("anio") * 100 + F.col("mes")).alias("b")).first()
print("Hechos: %d filas distrito-mes (%d-%02d..%d-%02d) | alertas=%d | meses con interrupcion real=%d"
      % (r["n"], r["a"] // 100, r["a"] % 100, r["b"] // 100, r["b"] % 100, r["al"], r["re"]))
print("Datos personales: ninguno (panel agregado distrito-mes; sin IDs ni nombres de personas).")
spark.stop()
