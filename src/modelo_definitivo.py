"""4. Modelo definitivo: validacion temporal rolling-origin, seleccion por AUC-PR y holdout 2024.

Ejecutar (despues de integracion_clima.py):
    docker exec spark_node sh -c '/spark/bin/spark-submit /app/src/modelo_definitivo.py 2>&1 | grep -v " INFO " | grep -v "^WARN" | tee /app/evidencias/04_modelo_definitivo.txt'

Responde a la observacion 1 (seleccion inconsistente, M2 sin reportar, margen sobre 5 meses):
* Panel: processed/master_panel_clima.parquet (todos los distritos; La Libertad con NASA POWER).
* Variantes sobre la MISMA poblacion (todos los distritos):
    M1 sin clima | M2 = M1 + PP_ANOMALIA_LAG1, MES_LLUVIOSO_LAG1, CLIMA_DISPONIBLE
  (sin clima: PP_ANOMALIA_LAG1 = 1.0 = mes normal, MES_LLUVIOSO_LAG1 = 0, CLIMA_DISPONIBLE = 0).
* Modelos: regla ingenua (score = TARGET_LAG1, "hubo interrupcion el mes anterior"),
  LogisticRegression, DecisionTree, RandomForest, GBT. Hiperparametros fijos (no se ajustan).
* pyspark.ml.Pipeline: StringIndexer -> OneHotEncoder -> Imputer -> VectorAssembler ->
  StandardScaler -> modelo. Todo se ajusta solo con el entrenamiento de cada corte.
* Rolling-origin (ventana de entrenamiento creciente), 4 cortes semestrales:
    2022-S1, 2022-S2, 2023-S1, 2023-S2 (entrena con todos los meses anteriores al semestre).
  Holdout final: entrena con todo < 2024-01 y prueba en 2024-01..2024-05.
* Seleccion: mayor AUC-PR promedio de los 4 cortes (nacional). El holdout NO participa.
* Umbral: sobre las predicciones de validacion de los 4 cortes agrupadas, el que maximiza el
  recall con precision >= 0.60 (empate: mayor precision, luego mayor umbral). Se aplica fijo al
  holdout.
* Margen frente a la regla ingenua en holdout: bootstrap por conglomerados (se remuestrean
  DISTRITOS con reemplazo, B=1000, semilla 42), IC 95 % por percentiles, pareado.
* AUC-PR implementada como Spark BinaryClassificationMetrics (curva PR por umbral distinto,
  punto inicial (0, precision del primer umbral), trapecios); se verifica contra el evaluador.
"""
import csv
import os

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier, GBTClassifier, LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

import comun as C
from comun import titulo

PANEL_CLIMA = C.PROCESSED + "master_panel_clima.parquet"
EVID = os.environ.get("SEDALIB_EVID", "/app/evidencias/")   # AWS: carpeta local del driver (aws/emr_lanzador.py)
LL = "LA LIBERTAD"
SEED, B, PREC_MIN = 42, 1000, 0.60
CLAVE = ["DEPTO_N", "DIST_N", "ANIO", "MES"]

CATEG = ["MES_STR", "DEPTO_N", "TIPO_PELIGRO"]
NUM_M1 = ["INT_LAG1", "TARGET_LAG1", "INT_PREV3", "N_PELIGROS", "PCT_SIST_MAS_20_ANIOS",
          "PCT_SIST_NO_OPERA", "INFRA_DISPONIBLE"]
NUM_CLIMA = ["PP_ANOMALIA_LAG1", "MES_LLUVIOSO_LAG1", "CLIMA_DISPONIBLE"]
IMPUTAR = ["PCT_SIST_MAS_20_ANIOS", "PCT_SIST_NO_OPERA"]
VARIANTES = [("M1", NUM_M1), ("M2", NUM_M1 + NUM_CLIMA)]

def T():
    return F.col("ANIO") * 12 + F.col("MES") - 1


def t(anio, mes):
    return anio * 12 + mes - 1


CORTES = [("2022-S1", t(2022, 1), t(2022, 6)), ("2022-S2", t(2022, 7), t(2022, 12)),
          ("2023-S1", t(2023, 1), t(2023, 6)), ("2023-S2", t(2023, 7), t(2023, 12))]
HOLDOUT = ("2024-01..05", t(2024, 1), t(2024, 5))
MODELOS = ["Regla ingenua", "LogisticRegression", "DecisionTree", "RandomForest", "GBT"]


def clasificador(nombre):
    k = dict(featuresCol="features_std", labelCol="label")
    if nombre == "LogisticRegression":
        return LogisticRegression(maxIter=100, **k)
    if nombre == "DecisionTree":
        return DecisionTreeClassifier(maxDepth=5, minInstancesPerNode=20, seed=SEED, **k)
    if nombre == "RandomForest":
        return RandomForestClassifier(numTrees=100, maxDepth=8, minInstancesPerNode=20, seed=SEED, **k)
    return GBTClassifier(maxIter=50, maxDepth=4, stepSize=0.1, seed=SEED, **k)


def pipeline(numericas, nombre):
    otras = [c for c in numericas if c not in IMPUTAR]
    return Pipeline(stages=[
        StringIndexer(inputCols=CATEG, outputCols=[c + "_idx" for c in CATEG], handleInvalid="keep"),
        OneHotEncoder(inputCols=[c + "_idx" for c in CATEG], outputCols=[c + "_oh" for c in CATEG],
                      handleInvalid="keep"),
        Imputer(strategy="median", inputCols=IMPUTAR, outputCols=[c + "_imp" for c in IMPUTAR]),
        VectorAssembler(inputCols=otras + [c + "_imp" for c in IMPUTAR] + [c + "_oh" for c in CATEG],
                        outputCol="features"),
        StandardScaler(inputCol="features", outputCol="features_std"),
        clasificador(nombre)])


# ----------------------------------------------------------------- metricas (numpy)
def _curva(y, s):
    o = np.argsort(-s, kind="mergesort")
    y, s = y[o], s[o]
    tp, fp = np.cumsum(y), np.cumsum(1 - y)
    idx = np.r_[np.where(np.diff(s) != 0)[0], len(s) - 1]       # ultimo indice de cada umbral distinto
    return s[idx], tp[idx], fp[idx]


def auc_pr(y, s):
    P = y.sum()
    if P == 0:
        return float("nan")
    _, tp, fp = _curva(y, s)
    rec, prec = tp / P, tp / (tp + fp)
    return float(np.trapz(np.r_[prec[0], prec], np.r_[0.0, rec]))


def auc_roc(y, s):
    P, N = y.sum(), len(y) - y.sum()
    if P == 0 or N == 0:
        return float("nan")
    _, tp, fp = _curva(y, s)
    return float(np.trapz(np.r_[0.0, tp / P, 1.0], np.r_[0.0, fp / N, 1.0]))


def elegir_umbral(y, s):
    """Maximiza recall con precision >= PREC_MIN; empate -> mayor precision -> mayor umbral."""
    thr, tp, fp = _curva(y, s)
    prec, rec = tp / (tp + fp), tp / y.sum()
    ok = np.where(prec >= PREC_MIN)[0]
    if len(ok) == 0:
        i = int(np.argmax(prec))
        return float(thr[i]), False
    i = sorted(ok, key=lambda k: (-rec[k], -prec[k], -thr[k]))[0]
    return float(thr[i]), True


def matriz(y, s, u):
    p = (s >= u).astype(int)
    tp = int(((p == 1) & (y == 1)).sum()); fp = int(((p == 1) & (y == 0)).sum())
    fn = int(((p == 0) & (y == 1)).sum()); tn = int(((p == 0) & (y == 0)).sum())
    prec = tp / (tp + fp) if tp + fp else float("nan")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    return dict(tn=tn, fp=fp, fn=fn, tp=tp, precision=prec, recall=rec)


# ----------------------------------------------------------------- datos
spark = C.spark_session("SEDALIB_ModeloDefinitivo", **{"spark.sql.shuffle.partitions": "16"})
datos = (spark.read.parquet(PANEL_CLIMA).filter(F.col("INT_LAG1").isNotNull())
         .withColumn("label", F.col("TARGET").cast("double"))
         .withColumn("T", T())
         .withColumn("MES_STR", F.format_string("%02d", F.col("MES")))
         .withColumn("TIPO_PELIGRO", F.coalesce(F.col("TIPO_PELIGRO_PRINCIPAL"), F.lit("SIN_DATO")))
         .withColumn("PP_ANOMALIA_LAG1", F.coalesce(F.col("PP_ANOMALIA_LAG1"), F.lit(1.0)))
         .withColumn("MES_LLUVIOSO_LAG1", F.coalesce(F.col("MES_LLUVIOSO_LAG1"), F.lit(0))))
for c in NUM_M1 + NUM_CLIMA:
    datos = datos.withColumn(c, F.col(c).cast("double"))
datos = datos.select(*CLAVE, "T", "label", *CATEG, *sorted(set(NUM_M1 + NUM_CLIMA))).cache()

titulo("4.0 DATOS Y DISENO")
r = datos.agg(F.count("*").alias("n"), F.countDistinct("DEPTO_N", "DIST_N").alias("d"),
              F.avg("label").alias("prev"), F.min("T").alias("a"), F.max("T").alias("b")).first()
print("Filas modelables (con INT_LAG1): %d | distritos: %d | prevalencia: %.4f | %d-%02d a %d-%02d"
      % (r["n"], r["d"], r["prev"], r["a"] // 12, r["a"] % 12 + 1, r["b"] // 12, r["b"] % 12 + 1))
print("Variables M1: %s + categoricas %s" % (NUM_M1, CATEG))
print("Variables M2: M1 + %s" % NUM_CLIMA)
print("Excluidas por fuga: N_INTERRUPCIONES, N_IMPREVISTAS, TARGET y clima del mismo mes.")
print("\n%-12s %-22s %-22s %8s %8s %10s %10s" % ("corte", "entrenamiento", "validacion", "n_train", "n_val",
                                                  "prev_tr", "prev_val"))
for nombre, a, b in CORTES + [HOLDOUT]:
    tr = datos.filter(F.col("T") < a); va = datos.filter((F.col("T") >= a) & (F.col("T") <= b))
    print("%-12s %-22s %-22s %8d %8d %10.4f %10.4f" % (
        nombre, "2019-07..%d-%02d" % ((a - 1) // 12, (a - 1) % 12 + 1),
        "%d-%02d..%d-%02d" % (a // 12, a % 12 + 1, b // 12, b % 12 + 1), tr.count(), va.count(),
        tr.agg(F.avg("label")).first()[0], va.agg(F.avg("label")).first()[0]))


def predecir(a, b):
    """Entrena con T < a, predice T en [a, b]. Devuelve {(variante, modelo): arrays} + claves."""
    tr = datos.filter(F.col("T") < a).cache()
    va = datos.filter((F.col("T") >= a) & (F.col("T") <= b)).cache()
    base = va.select(*CLAVE, "label", "TARGET_LAG1").orderBy(*CLAVE).collect()
    info = dict(depto=np.array([x["DEPTO_N"] for x in base]), dist=np.array([x["DIST_N"] for x in base]),
                anio=np.array([x["ANIO"] for x in base]), mes=np.array([x["MES"] for x in base]),
                y=np.array([int(x["label"]) for x in base]))
    preds = {}
    for var, _ in VARIANTES:
        preds[(var, "Regla ingenua")] = np.array([float(x["TARGET_LAG1"]) for x in base])
    for var, num in VARIANTES:
        for m in MODELOS[1:]:
            pm = pipeline(num, m).fit(tr)
            out = (pm.transform(va).select(*CLAVE, vector_to_array("probability")[1].alias("p"))
                   .orderBy(*CLAVE).collect())
            preds[(var, m)] = np.array([x["p"] for x in out])
    tr.unpersist(); va.unpersist()
    return info, preds, va


# ----------------------------------------------------------------- rolling-origin
titulo("4.1 VALIDACION ROLLING-ORIGIN (4 cortes semestrales) - AUC-PR / AUC-ROC nacional")
filas_csv = []
cv = {}            # (var, modelo) -> lista auc_pr nacional por corte
cv_ll = {}
pool = {}          # (var, modelo) -> (y, s) agrupado de validacion
for nombre, a, b in CORTES:
    info, preds, va_df = predecir(a, b)
    y, es_ll = info["y"], info["depto"] == LL
    print("\nCorte %s (n=%d, positivos=%d; La Libertad n=%d, positivos=%d)"
          % (nombre, len(y), y.sum(), es_ll.sum(), y[es_ll].sum()))
    for (var, m), s in sorted(preds.items(), key=lambda kv: (kv[0][0], MODELOS.index(kv[0][1]))):
        ap, ar = auc_pr(y, s), auc_roc(y, s)
        ap_ll = auc_pr(y[es_ll], s[es_ll])
        cv.setdefault((var, m), []).append(ap); cv_ll.setdefault((var, m), []).append(ap_ll)
        py, ps = pool.get((var, m), (np.array([], int), np.array([])))
        pool[(var, m)] = (np.r_[py, y], np.r_[ps, s])
        print("   %-3s %-20s AUC-PR=%.4f  AUC-ROC=%.4f | La Libertad AUC-PR=%.4f" % (var, m, ap, ar, ap_ll))
        filas_csv.append(dict(variante=var, modelo=m, conjunto="cv_" + nombre, ambito="nacional",
                              n=len(y), positivos=int(y.sum()), auc_pr=ap, auc_roc=ar))
        filas_csv.append(dict(variante=var, modelo=m, conjunto="cv_" + nombre, ambito="la_libertad",
                              n=int(es_ll.sum()), positivos=int(y[es_ll].sum()), auc_pr=ap_ll,
                              auc_roc=auc_roc(y[es_ll], s[es_ll])))
    if nombre == CORTES[0][0]:          # control de la implementacion numpy contra Spark
        chk = spark.createDataFrame([(float(a_), float(b_)) for a_, b_ in zip(y, preds[("M1", "GBT")])],
                                    ["label", "p"])
        ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="p")
        print("   Control M1-GBT: AUC-PR numpy=%.6f Spark=%.6f | AUC-ROC numpy=%.6f Spark=%.6f"
              % (auc_pr(y, preds[("M1", "GBT")]), ev.setMetricName("areaUnderPR").evaluate(chk),
                 auc_roc(y, preds[("M1", "GBT")]), ev.setMetricName("areaUnderROC").evaluate(chk)))

titulo("4.2 RESUMEN CV: AUC-PR media y desviacion (ddof=1) de los 4 cortes")
print("%-3s %-20s %10s %10s %12s %12s %12s" % ("var", "modelo", "media", "desv", "min", "max", "media_LL"))
resumen = []
for (var, m), v in sorted(cv.items(), key=lambda kv: -np.mean(kv[1])):
    v = np.array(v)
    resumen.append((var, m, v.mean(), v.std(ddof=1)))
    print("%-3s %-20s %10.4f %10.4f %12.4f %12.4f %12.4f"
          % (var, m, v.mean(), v.std(ddof=1), v.min(), v.max(), np.nanmean(cv_ll[(var, m)])))
    for amb, vals in (("nacional", v), ("la_libertad", np.array(cv_ll[(var, m)]))):
        filas_csv.append(dict(variante=var, modelo=m, conjunto="cv_media", ambito=amb, auc_pr=np.nanmean(vals)))
        filas_csv.append(dict(variante=var, modelo=m, conjunto="cv_desv", ambito=amb, auc_pr=np.nanstd(vals, ddof=1)))
sel_var, sel_mod = resumen[0][0], resumen[0][1]
print("\nSELECCIONADO (mayor AUC-PR media en CV, sin mirar el holdout): %s - %s" % (sel_var, sel_mod))
for var, _ in VARIANTES:
    mv = [x for x in resumen if x[0] == var][0]
    print("   Mejor de %s: %s (%.4f +/- %.4f)" % (var, mv[1], mv[2], mv[3]))
for m in MODELOS:
    a1, a2 = np.array(cv[("M1", m)]), np.array(cv[("M2", m)])
    print("   M2 - M1 %-20s por corte: %s | media %+.4f" % (m, " ".join("%+.4f" % d for d in a2 - a1), (a2 - a1).mean()))

titulo("4.3 UMBRAL elegido en validacion agrupada (recall max con precision >= %.2f)" % PREC_MIN)
umbral = {}
print("%-3s %-20s %10s %10s %10s %10s" % ("var", "modelo", "umbral", "precision", "recall", "factible"))
for (var, m), (y, s) in sorted(pool.items(), key=lambda kv: (kv[0][0], MODELOS.index(kv[0][1]))):
    u, ok = elegir_umbral(y, s)
    umbral[(var, m)] = u
    mt = matriz(y, s, u)
    print("%-3s %-20s %10.4f %10.4f %10.4f %10s" % (var, m, u, mt["precision"], mt["recall"], "si" if ok else "NO"))
    filas_csv.append(dict(variante=var, modelo=m, conjunto="validacion_agrupada", ambito="nacional", n=len(y),
                          positivos=int(y.sum()), auc_pr=auc_pr(y, s), auc_roc=auc_roc(y, s), umbral=u,
                          umbral_factible=int(ok), **mt))

# ----------------------------------------------------------------- holdout
titulo("4.4 HOLDOUT 2024-01..2024-05 (entrena con todo < 2024-01; umbral fijo de 4.3)")
info, preds, _ = predecir(HOLDOUT[1], HOLDOUT[2])
y_all = info["y"]
dist_id = np.array(["%s|%s" % (d, t_) for d, t_ in zip(info["depto"], info["dist"])])
rng = np.random.RandomState(SEED)


def bootstrap(mask):
    """Indices de B remuestreos por conglomerado de distrito dentro de mask."""
    idx = np.where(mask)[0]
    grupos = {}
    for i in idx:
        grupos.setdefault(dist_id[i], []).append(i)
    claves = sorted(grupos)
    arr = [np.array(grupos[k]) for k in claves]
    return [np.concatenate([arr[j] for j in rng.randint(0, len(arr), len(arr))]) for _ in range(B)]


pred_rows = []
for ambito, mask in (("nacional", np.ones(len(y_all), bool)), ("la_libertad", info["depto"] == LL)):
    y = y_all[mask]
    print("\n--- Ambito: %s | n=%d distrito-meses | distritos=%d | positivos=%d (prevalencia %.4f)"
          % (ambito.upper(), len(y), len(set(dist_id[mask])), y.sum(), y.mean()))
    reps = bootstrap(mask)
    print("%-3s %-20s %8s %8s %8s %8s %8s | %5s %5s %5s %5s | %-30s %-30s %-30s"
          % ("var", "modelo", "AUC-PR", "AUC-ROC", "umbral", "prec", "recall", "TN", "FP", "FN", "TP",
             "dAUC-PR vs ingenua [IC95]", "dRecall vs ingenua [IC95]", "dPrecision vs ingenua [IC95]"))
    for var, _ in VARIANTES:
        s_ing = preds[(var, "Regla ingenua")]
        for m in MODELOS:
            s_all = preds[(var, m)]
            s = s_all[mask]
            u = umbral[(var, m)]
            ap, ar, mt = auc_pr(y, s), auc_roc(y, s), matriz(y, s, u)
            mi = matriz(y, s_ing[mask], umbral[(var, "Regla ingenua")])
            d_ap = ap - auc_pr(y, s_ing[mask])
            d_rec, d_prec = mt["recall"] - mi["recall"], mt["precision"] - mi["precision"]
            if m == "Regla ingenua":
                ic = dict(ic_auc_pr=(np.nan, np.nan), ic_rec=(np.nan, np.nan), ic_prec=(np.nan, np.nan))
            else:
                bs_ap, bs_rec, bs_prec = [], [], []
                ui = umbral[(var, "Regla ingenua")]
                for ix in reps:
                    yy = y_all[ix]
                    bs_ap.append(auc_pr(yy, s_all[ix]) - auc_pr(yy, s_ing[ix]))
                    a_, b_ = matriz(yy, s_all[ix], u), matriz(yy, s_ing[ix], ui)
                    bs_rec.append(a_["recall"] - b_["recall"]); bs_prec.append(a_["precision"] - b_["precision"])
                pc = lambda v: tuple(np.nanpercentile(np.array(v, float), [2.5, 97.5]))
                ic = dict(ic_auc_pr=pc(bs_ap), ic_rec=pc(bs_rec), ic_prec=pc(bs_prec))
            fmt = lambda d, c: ("%+.4f [%+.4f, %+.4f]" % (d, c[0], c[1])) if m != "Regla ingenua" else "-"
            marca = " <== seleccionado" if (var, m) == (sel_var, sel_mod) else ""
            print("%-3s %-20s %8.4f %8.4f %8.4f %8.4f %8.4f | %5d %5d %5d %5d | %-30s %-30s %-30s%s"
                  % (var, m, ap, ar, u, mt["precision"], mt["recall"], mt["tn"], mt["fp"], mt["fn"], mt["tp"],
                     fmt(d_ap, ic["ic_auc_pr"]), fmt(d_rec, ic["ic_rec"]), fmt(d_prec, ic["ic_prec"]), marca))
            filas_csv.append(dict(variante=var, modelo=m, conjunto="holdout_2024", ambito=ambito, n=len(y),
                                  positivos=int(y.sum()), auc_pr=ap, auc_roc=ar, umbral=u, **mt,
                                  delta_auc_pr_vs_ingenua=d_ap, ic95_inf_delta_auc_pr=ic["ic_auc_pr"][0],
                                  ic95_sup_delta_auc_pr=ic["ic_auc_pr"][1], delta_recall_vs_ingenua=d_rec,
                                  ic95_inf_delta_recall=ic["ic_rec"][0], ic95_sup_delta_recall=ic["ic_rec"][1],
                                  delta_precision_vs_ingenua=d_prec, ic95_inf_delta_precision=ic["ic_prec"][0],
                                  ic95_sup_delta_precision=ic["ic_prec"][1],
                                  seleccionado=int((var, m) == (sel_var, sel_mod))))

ll = info["depto"] == LL
print("\nLa Libertad - holdout por distrito (modelo seleccionado %s-%s, umbral %.4f):"
      % (sel_var, sel_mod, umbral[(sel_var, sel_mod)]))
print("%-24s %6s %8s %8s %8s" % ("distrito", "meses", "reales", "pred=1", "p_media"))
s_sel = preds[(sel_var, sel_mod)]
for d in sorted(set(info["dist"][ll])):
    k = ll & (info["dist"] == d)
    print("%-24s %6d %8d %8d %8.4f" % (d, k.sum(), y_all[k].sum(), (s_sel[k] >= umbral[(sel_var, sel_mod)]).sum(),
                                       s_sel[k].mean()))

# ----------------------------------------------------------------- salidas
cols = ["variante", "modelo", "conjunto", "ambito", "n", "positivos", "auc_pr", "auc_roc", "umbral",
        "umbral_factible", "precision", "recall", "tn", "fp", "fn", "tp", "delta_auc_pr_vs_ingenua",
        "ic95_inf_delta_auc_pr", "ic95_sup_delta_auc_pr", "delta_recall_vs_ingenua", "ic95_inf_delta_recall",
        "ic95_sup_delta_recall", "delta_precision_vs_ingenua", "ic95_inf_delta_precision",
        "ic95_sup_delta_precision", "seleccionado"]
with open(EVID + "metricas_modelos.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for fila in filas_csv:
        w.writerow({k: (round(v, 6) if isinstance(v, float) else v) for k, v in fila.items()})
with open(EVID + "modelo_definitivo_pred_holdout.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    claves = sorted(preds, key=lambda k: (k[0], MODELOS.index(k[1])))
    w.writerow(["DEPTO_N", "DIST_N", "ANIO", "MES", "label"] + ["%s|%s" % k for k in claves])
    for i in range(len(y_all)):
        w.writerow([info["depto"][i], info["dist"][i], int(info["anio"][i]), int(info["mes"][i]), int(y_all[i])]
                   + [round(float(preds[k][i]), 6) for k in claves])
print("\nEscrito: evidencias/metricas_modelos.csv (%d filas), evidencias/modelo_definitivo_pred_holdout.csv (%d filas)"
      % (len(filas_csv), len(y_all)))
spark.stop()
