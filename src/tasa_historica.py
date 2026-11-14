"""4b. Linea base de TASA HISTORICA por distrito vs M1-GBT (verificacion; no modifica el modelo).

score(distrito, mes) = media de TARGET del distrito en los meses de entrenamiento (T < inicio del corte),
sobre la misma poblacion de modelo_definitivo.py (filas con INT_LAG1 no nulo).
Umbral: mismo criterio que el resto (validacion rolling-origin agrupada de los 4 cortes, recall maximo
con precision >= 0.60), aplicado fijo al holdout 2024-01..05.
IC 95 %: mismo bootstrap por conglomerados de distrito (B=1000, semilla 42, mismos remuestreos).
"""
import sys
sys.path.insert(0, "/app/src")
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
import comun as C
from comun import titulo

PANEL_CLIMA = C.PROCESSED + "master_panel_clima.parquet"
LL = "LA LIBERTAD"
SEED, B, PREC_MIN = 42, 1000, 0.60
CLAVE = ["DEPTO_N", "DIST_N", "ANIO", "MES"]
CATEG = ["MES_STR", "DEPTO_N", "TIPO_PELIGRO"]
NUM_M1 = ["INT_LAG1", "TARGET_LAG1", "INT_PREV3", "N_PELIGROS", "PCT_SIST_MAS_20_ANIOS",
          "PCT_SIST_NO_OPERA", "INFRA_DISPONIBLE"]
NUM_CLIMA = ["PP_ANOMALIA_LAG1", "MES_LLUVIOSO_LAG1", "CLIMA_DISPONIBLE"]
IMPUTAR = ["PCT_SIST_MAS_20_ANIOS", "PCT_SIST_NO_OPERA"]


def t(anio, mes):
    return anio * 12 + mes - 1


CORTES = [("2022-S1", t(2022, 1), t(2022, 6)), ("2022-S2", t(2022, 7), t(2022, 12)),
          ("2023-S1", t(2023, 1), t(2023, 6)), ("2023-S2", t(2023, 7), t(2023, 12))]
HOLDOUT = ("2024-01..05", t(2024, 1), t(2024, 5))


def pipeline_gbt():
    otras = [c for c in NUM_M1 if c not in IMPUTAR]
    return Pipeline(stages=[
        StringIndexer(inputCols=CATEG, outputCols=[c + "_idx" for c in CATEG], handleInvalid="keep"),
        OneHotEncoder(inputCols=[c + "_idx" for c in CATEG], outputCols=[c + "_oh" for c in CATEG],
                      handleInvalid="keep"),
        Imputer(strategy="median", inputCols=IMPUTAR, outputCols=[c + "_imp" for c in IMPUTAR]),
        VectorAssembler(inputCols=otras + [c + "_imp" for c in IMPUTAR] + [c + "_oh" for c in CATEG],
                        outputCol="features"),
        StandardScaler(inputCol="features", outputCol="features_std"),
        GBTClassifier(maxIter=50, maxDepth=4, stepSize=0.1, seed=SEED, featuresCol="features_std",
                      labelCol="label")])


# --- metricas identicas a modelo_definitivo.py
def _curva(y, s):
    o = np.argsort(-s, kind="mergesort")
    y, s = y[o], s[o]
    tp, fp = np.cumsum(y), np.cumsum(1 - y)
    idx = np.r_[np.where(np.diff(s) != 0)[0], len(s) - 1]
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


# --- datos (misma preparacion que modelo_definitivo.py)
spark = C.spark_session("SEDALIB_TasaHistorica", **{"spark.sql.shuffle.partitions": "16"})
datos = (spark.read.parquet(PANEL_CLIMA).filter(F.col("INT_LAG1").isNotNull())
         .withColumn("label", F.col("TARGET").cast("double"))
         .withColumn("T", F.col("ANIO") * 12 + F.col("MES") - 1)
         .withColumn("MES_STR", F.format_string("%02d", F.col("MES")))
         .withColumn("TIPO_PELIGRO", F.coalesce(F.col("TIPO_PELIGRO_PRINCIPAL"), F.lit("SIN_DATO")))
         .withColumn("PP_ANOMALIA_LAG1", F.coalesce(F.col("PP_ANOMALIA_LAG1"), F.lit(1.0)))
         .withColumn("MES_LLUVIOSO_LAG1", F.coalesce(F.col("MES_LLUVIOSO_LAG1"), F.lit(0))))
for c in NUM_M1 + NUM_CLIMA:
    datos = datos.withColumn(c, F.col(c).cast("double"))
datos = datos.select(*CLAVE, "T", "label", *CATEG, *sorted(set(NUM_M1 + NUM_CLIMA))).cache()


def tasa_historica(a, b):
    """score = tasa de interrupcion del distrito en T < a. Distrito sin historia -> prevalencia global de T < a."""
    tr = datos.filter(F.col("T") < a)
    prev = tr.agg(F.avg("label")).first()[0]
    tasa = tr.groupBy("DEPTO_N", "DIST_N").agg(F.avg("label").alias("tasa"), F.count("*").alias("meses_hist"))
    va = (datos.filter((F.col("T") >= a) & (F.col("T") <= b)).join(tasa, ["DEPTO_N", "DIST_N"], "left")
          .orderBy(*CLAVE).collect())
    sin = sum(1 for x in va if x["tasa"] is None)
    s = np.array([prev if x["tasa"] is None else float(x["tasa"]) for x in va])
    info = dict(depto=np.array([x["DEPTO_N"] for x in va]), dist=np.array([x["DIST_N"] for x in va]),
                y=np.array([int(x["label"]) for x in va]), lag1=np.array([float(x["TARGET_LAG1"]) for x in va]),
                meses=np.array([0 if x["meses_hist"] is None else x["meses_hist"] for x in va]))
    return info, s, sin, prev


titulo("4b.0 DEFINICION")
print("Regla ingenua (modelo_definitivo.py, linea 180): score = TARGET_LAG1 (1 si hubo interrupcion el mes anterior).")
print("Tasa historica: score = mean(TARGET) del distrito en todos los meses de entrenamiento (T < inicio del corte),")
print("  misma poblacion (INT_LAG1 no nulo). Distrito sin historia en entrenamiento -> prevalencia global de entrenamiento.")
print("Umbral: validacion rolling-origin agrupada (4 cortes), recall max con precision >= %.2f; fijo en holdout." % PREC_MIN)

titulo("4b.1 VALIDACION ROLLING-ORIGIN - Tasa historica")
py, ps, piny, pins = [], [], [], []
for nombre, a, b in CORTES:
    info, s, sin, prev = tasa_historica(a, b)
    y = info["y"]; ll = info["depto"] == LL
    print("Corte %s n=%d pos=%d | sin historia=%d | AUC-PR=%.4f AUC-ROC=%.4f | LL AUC-PR=%.4f | (ingenua AUC-PR=%.4f)"
          % (nombre, len(y), y.sum(), sin, auc_pr(y, s), auc_roc(y, s), auc_pr(y[ll], s[ll]), auc_pr(y, info["lag1"])))
    py.append(y); ps.append(s); piny.append(y); pins.append(info["lag1"])
py, ps = np.concatenate(py), np.concatenate(ps)
u_th, ok = elegir_umbral(py, ps)
mv = matriz(py, ps, u_th)
u_ing, _ = elegir_umbral(np.concatenate(piny), np.concatenate(pins))
print("\nUmbral tasa historica (validacion agrupada n=%d): %.4f | precision=%.4f recall=%.4f factible=%s"
      % (len(py), u_th, mv["precision"], mv["recall"], "si" if ok else "NO"))
print("Control: umbral regla ingenua recalculado = %.4f (esperado 1.0000)" % u_ing)
U_GBT = 0.1102   # umbral M1-GBT reportado en evidencias/04_modelo_definitivo.txt (seccion 4.3)

titulo("4b.2 HOLDOUT 2024-01..05 - Tasa historica vs M1-GBT (y regla ingenua como referencia)")
a, b = HOLDOUT[1], HOLDOUT[2]
info, s_th, sin, prev = tasa_historica(a, b)
y_all = info["y"]; s_ing = info["lag1"]
tr = datos.filter(F.col("T") < a); va = datos.filter((F.col("T") >= a) & (F.col("T") <= b))
out = (pipeline_gbt().fit(tr).transform(va).select(*CLAVE, vector_to_array("probability")[1].alias("p"))
       .orderBy(*CLAVE).collect())
s_gbt = np.array([x["p"] for x in out])
print("Filas holdout=%d | distritos sin historia de entrenamiento=%d (score=prevalencia %.4f) | meses de historia: min=%d max=%d"
      % (len(y_all), sin, prev, info["meses"].min(), info["meses"].max()))
print("Control M1-GBT reentrenado: AUC-PR nacional=%.4f (esperado 0.7293), umbral fijo %.4f" % (auc_pr(y_all, s_gbt), U_GBT))

dist_id = np.array(["%s|%s" % (d, t_) for d, t_ in zip(info["depto"], info["dist"])])
rng = np.random.RandomState(SEED)


def bootstrap(mask):
    idx = np.where(mask)[0]
    grupos = {}
    for i in idx:
        grupos.setdefault(dist_id[i], []).append(i)
    arr = [np.array(grupos[k]) for k in sorted(grupos)]
    return [np.concatenate([arr[j] for j in rng.randint(0, len(arr), len(arr))]) for _ in range(B)]


pc = lambda v: tuple(np.nanpercentile(np.array(v, float), [2.5, 97.5]))
for ambito, mask in (("nacional", np.ones(len(y_all), bool)), ("la_libertad", info["depto"] == LL)):
    reps = bootstrap(mask)          # misma secuencia de remuestreos que modelo_definitivo.py
    y = y_all[mask]
    print("\n--- Ambito: %s | n=%d | distritos=%d | positivos=%d (prevalencia %.4f)"
          % (ambito.upper(), len(y), len(set(dist_id[mask])), y.sum(), y.mean()))
    print("%-18s %8s %8s %8s %8s %8s | %4s %4s %4s %4s" % ("modelo", "AUC-PR", "AUC-ROC", "umbral", "prec",
                                                         "recall", "TN", "FP", "FN", "TP"))
    for nom, s, u in (("Tasa historica", s_th, u_th), ("Regla ingenua", s_ing, 1.0), ("M1-GBT", s_gbt, U_GBT)):
        m = matriz(y, s[mask], u)
        print("%-18s %8.4f %8.4f %8.4f %8.4f %8.4f | %4d %4d %4d %4d" % (nom, auc_pr(y, s[mask]), auc_roc(y, s[mask]),
              u, m["precision"], m["recall"], m["tn"], m["fp"], m["fn"], m["tp"]))
    mg, mt = matriz(y, s_gbt[mask], U_GBT), matriz(y, s_th[mask], u_th)
    d = dict(ap=auc_pr(y, s_gbt[mask]) - auc_pr(y, s_th[mask]), rec=mg["recall"] - mt["recall"],
             prec=mg["precision"] - mt["precision"])
    bs = dict(ap=[], rec=[], prec=[])
    for ix in reps:
        yy = y_all[ix]
        bs["ap"].append(auc_pr(yy, s_gbt[ix]) - auc_pr(yy, s_th[ix]))
        a_, b_ = matriz(yy, s_gbt[ix], U_GBT), matriz(yy, s_th[ix], u_th)
        bs["rec"].append(a_["recall"] - b_["recall"]); bs["prec"].append(a_["precision"] - b_["precision"])
    print("M1-GBT - Tasa historica (bootstrap por distrito, B=%d, semilla %d, IC95 percentil):" % (B, SEED))
    for k, lab in (("ap", "dAUC-PR"), ("rec", "dRecall"), ("prec", "dPrecision")):
        lo, hi = pc(bs[k])
        print("   %-11s %+.4f  [%+.4f, %+.4f]%s" % (lab, d[k], lo, hi, "  (IC incluye 0)" if lo <= 0 <= hi else ""))

spark.stop()
