"""6. Impacto por distrito + tasa historica por distrito-mes para el dashboard (no modifica el modelo ni las
tablas gold existentes).

python src/gold_impacto.py --panel <ruta local de processed/master_panel_clima.parquet>   (en el host, pandas)

Salidas
  curated/gold_impacto_distrito/gold_impacto_distrito.parquet  (local: data/curated/..., luego se sube a S3)
  dashboard/data/gold_impacto_distrito.csv
  dashboard/data/tasa_historica_distrito_mes.csv
  evidencias/06_impacto_distrito.txt

gold_impacto_distrito (fuente: raw/operaciones/sunass_interrupciones.csv)
* Misma limpieza que procesamiento_spark.py: dropDuplicates, dedup (IDINTERRUPCION, depto, distrito),
  fecha inicio/fin validas, distrito no nulo; nombres normalizados como comun.norm().
* Ventana pedida 2019-06..2023-12 (sin 2024). NUMCONEXDOM y UNIDADESUSO estan 100 % vacios hasta 2021,
  ~1 % informados en 2022-01..05 y 100 % desde 2022-07: la ventana efectiva con dato es 2022-01..2023-12.
* Valores imposibles: un registro con NUMCONEXDOM o UNIDADESUSO > 200 000 se trata como invalido (nulo).
  Ningun distrito del Peru tiene esa cantidad de conexiones (hay registros de hasta 21.6 millones).
* Promedio mensual por distrito = suma de afectados en el mes / meses con interrupcion y dato valido
  (impacto esperado si el distrito tiene interrupcion ese mes). Una conexion afectada por 2 interrupciones
  del mismo mes cuenta 2 veces (afectaciones, no conexiones distintas). Como referencia de conexiones
  distintas se guarda conexiones_max_evento_prom_mes (promedio del mayor evento de cada mes = cota inferior).

tasa_historica_distrito_mes: TASA_HIST exactamente como src/gold.py (mean(TARGET) del distrito en los meses
de entrenamiento de cada corte; si el distrito no tiene historia, la prevalencia del entrenamiento).
Control: AUC-PR holdout 2024 = 0.6858 nacional y 0.7336 La Libertad (gold_comparacion_modelos).
"""
import argparse
import os

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_INT = os.path.join(BASE, "data", "raw", "operaciones", "sunass_interrupciones.csv")
OUT_PQ = os.path.join(BASE, "data", "curated", "gold_impacto_distrito", "gold_impacto_distrito.parquet")
DASH = os.path.join(BASE, "dashboard", "data")
EVID = os.path.join(BASE, "evidencias", "06_impacto_distrito.txt")
MAX_VALIDO = 200_000
DESDE, HASTA = 2019 * 12 + 5, 2023 * 12 + 11            # 2019-06 .. 2023-12 (indice anio*12+mes-1)
CORTES = [("cv_2022-S1", 2022, 1, 2022, 6), ("cv_2022-S2", 2022, 7, 2022, 12), ("cv_2023-S1", 2023, 1, 2023, 6),
          ("cv_2023-S2", 2023, 7, 2023, 12), ("holdout_2024", 2024, 1, 2024, 5)]
LINEAS = []


def p(*a):
    s = " ".join(str(x) for x in a)
    print(s)
    LINEAS.append(s)


_TR = str.maketrans("ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ�_", "AEIOUAEIOUAEIOUNN ")


def norm(s):
    return s.str.strip().str.upper().str.translate(_TR).str.replace(r"\s+", " ", regex=True).str.strip()


_trapz = getattr(np, "trapezoid", None) or getattr(np, "trapz")


def auc_pr(y, s):                                         # igual que gold.py
    o = np.argsort(-s, kind="mergesort")
    y, s = y[o], s[o]
    tp, fp = np.cumsum(y), np.cumsum(1 - y)
    idx = np.r_[np.where(np.diff(s) != 0)[0], len(s) - 1]
    tp, fp = tp[idx], fp[idx]
    rec, prec = tp / y.sum(), tp / (tp + fp)
    return float(_trapz(np.r_[prec[0], prec], np.r_[0.0, rec]))


def impacto():
    p("=" * 100)
    p("6.1 CALIDAD DE NUMCONEXDOM Y UNIDADESUSO (raw/operaciones/sunass_interrupciones.csv)")
    p("=" * 100)
    d = pd.read_csv(RAW_INT, dtype=str, encoding="utf-8")
    n0 = len(d)
    p("filas raw: %d" % n0)
    for c in ["NUMCONEXDOM", "UNIDADESUSO"]:
        v = pd.to_numeric(d[c], errors="coerce")
        p("%-12s nulos %6d (%.2f %%) | no numericos %d | negativos %d | cero %d | no enteros %d | > %d: %d"
          % (c, v.isna().sum(), 100 * v.isna().mean(), (v.isna() & d[c].notna()).sum(), (v < 0).sum(),
             (v == 0).sum(), ((v % 1) != 0).sum(), MAX_VALIDO, (v > MAX_VALIDO).sum()))
    ym = d["FECHAINICIO"].str[:6]
    nul = pd.to_numeric(d["NUMCONEXDOM"], errors="coerce").isna().groupby(ym.str[:4]).mean().mul(100).round(1)
    p("%% de nulos por anio de inicio: %s" % ", ".join("%s %.1f %%" % kv for kv in nul.items()))
    con = pd.to_numeric(d["NUMCONEXDOM"], errors="coerce").notna().groupby(ym).mean()
    p("primer mes con dato: %s (%.0f %% informado); desde %s 100 %% informado"
      % (con[con > 0].index.min(), 100 * con[con > 0].iloc[0], con[con == 1].index.min()))

    # limpieza = procesamiento_spark.py
    d = d.drop_duplicates()
    d["DEPTO_N"], d["DIST_N"] = norm(d["DEPARTAMENTO"]), norm(d["DISTRITO"])
    d = d.drop_duplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"])
    fi = pd.to_datetime(d["FECHAINICIO"], format="%Y%m%d", errors="coerce")
    ff = pd.to_datetime(d["FECHAFIN"], format="%Y%m%d", errors="coerce")
    d = d[fi.notna() & ff.notna() & (ff >= fi) & d["DIST_N"].notna() & (d["DIST_N"] != "")].copy()
    d["T"] = fi[d.index].dt.year * 12 + fi[d.index].dt.month - 1
    d = d[(d["T"] >= DESDE) & (d["T"] <= HASTA)].copy()
    p("\ntras limpieza (igual que procesamiento_spark.py) y ventana 2019-06..2023-12: %d interrupciones" % len(d))
    for c in ["NUMCONEXDOM", "UNIDADESUSO"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    malo = (d["NUMCONEXDOM"] > MAX_VALIDO) | (d["UNIDADESUSO"] > MAX_VALIDO)
    p("registros con valor > %d (se anulan ambas columnas): %d" % (MAX_VALIDO, malo.sum()))
    p("   por EPS: %s" % d[malo]["EPS"].str.strip().value_counts().to_dict())
    d.loc[malo, ["NUMCONEXDOM", "UNIDADESUSO"]] = np.nan
    ok = d.dropna(subset=["NUMCONEXDOM", "UNIDADESUSO"])
    p("interrupciones con dato valido: %d de %d (%.1f %%); meses %d-%02d..%d-%02d"
      % (len(ok), len(d), 100 * len(ok) / len(d), ok["T"].min() // 12, ok["T"].min() % 12 + 1,
         ok["T"].max() // 12, ok["T"].max() % 12 + 1))

    m = ok.groupby(["DEPTO_N", "DIST_N", "T"]).agg(NUMCONEXDOM=("NUMCONEXDOM", "sum"),
                                                   UNIDADESUSO=("UNIDADESUSO", "sum"),
                                                   MAX_EVENTO=("NUMCONEXDOM", "max")).reset_index()
    g = m.groupby(["DEPTO_N", "DIST_N"]).agg(
        conexiones_prom_mes=("NUMCONEXDOM", "mean"), unidades_uso_prom_mes=("UNIDADESUSO", "mean"),
        conexiones_mediana_mes=("NUMCONEXDOM", "median"), conexiones_max_evento_prom_mes=("MAX_EVENTO", "mean"),
        meses_con_dato=("T", "nunique")).reset_index()
    g = g.rename(columns={"DEPTO_N": "departamento", "DIST_N": "distrito"})
    dims = pd.read_csv(os.path.join(DASH, "dim_distrito.csv"), dtype={"ubigeo": str})
    g = dims[["ubigeo", "departamento", "distrito"]].merge(g, on=["departamento", "distrito"], how="left")
    for c in ["conexiones_prom_mes", "unidades_uso_prom_mes", "conexiones_mediana_mes",
              "conexiones_max_evento_prom_mes"]:
        g[c] = g[c].round(1)
    g["meses_con_dato"] = g["meses_con_dato"].fillna(0).astype("int32")
    g["periodo_dato"] = "%d-%02d..%d-%02d" % (ok["T"].min() // 12, ok["T"].min() % 12 + 1, ok["T"].max() // 12,
                                             ok["T"].max() % 12 + 1)
    p("\n6.2 gold_impacto_distrito: %d distritos (los de dim_distrito); con dato %d, sin dato %d"
      % (len(g), (g.meses_con_dato > 0).sum(), (g.meses_con_dato == 0).sum()))
    ll = g[g.departamento == "LA LIBERTAD"].sort_values("conexiones_prom_mes", ascending=False)
    p("La Libertad:")
    p(ll[["distrito", "conexiones_prom_mes", "unidades_uso_prom_mes", "conexiones_mediana_mes",
          "conexiones_max_evento_prom_mes", "meses_con_dato"]].to_string(index=False))
    os.makedirs(os.path.dirname(OUT_PQ), exist_ok=True)
    g.to_parquet(OUT_PQ, index=False)
    g.to_csv(os.path.join(DASH, "gold_impacto_distrito.csv"), index=False)
    return g


def tasa(panel_path):
    p("\n" + "=" * 100)
    p("6.3 TASA HISTORICA POR DISTRITO-MES (misma definicion que src/gold.py)")
    p("=" * 100)
    pn = pd.read_parquet(panel_path, columns=["DEPTO_N", "DIST_N", "ANIO", "MES", "TARGET", "INT_LAG1"])
    pn = pn[pn["INT_LAG1"].notna()].copy()
    pn["T"] = pn["ANIO"].astype(int) * 12 + pn["MES"].astype(int) - 1
    pn["label"] = pn["TARGET"].astype(float)
    partes = []
    for nombre, a1, m1, a2, m2 in CORTES:
        a, b = a1 * 12 + m1 - 1, a2 * 12 + m2 - 1
        tr, va = pn[pn["T"] < a], pn[(pn["T"] >= a) & (pn["T"] <= b)].copy()
        t = tr.groupby(["DEPTO_N", "DIST_N"])["label"].mean().rename("TASA_HIST").reset_index()
        va = va.merge(t, on=["DEPTO_N", "DIST_N"], how="left")
        va["TASA_HIST"] = va["TASA_HIST"].fillna(tr["label"].mean())
        va["conjunto"] = nombre
        partes.append(va)
    r = pd.concat(partes)
    h = r[r["conjunto"] == "holdout_2024"]
    a_n = auc_pr(h["label"].values, h["TASA_HIST"].values)
    hl = h[h["DEPTO_N"] == "LA LIBERTAD"]
    a_l = auc_pr(hl["label"].values, hl["TASA_HIST"].values)
    p("control AUC-PR holdout 2024: nacional %.4f (esperado 0.6858) | La Libertad %.4f (esperado 0.7336)" % (a_n, a_l))
    assert abs(a_n - 0.6858) < 0.001 and abs(a_l - 0.7336) < 0.001, "tasa historica no reproduce gold"
    out = r.rename(columns={"DEPTO_N": "departamento", "DIST_N": "distrito", "ANIO": "anio", "MES": "mes",
                            "TASA_HIST": "prob_tasa_historica"})[
        ["departamento", "distrito", "anio", "mes", "prob_tasa_historica", "conjunto"]]
    out["prob_tasa_historica"] = out["prob_tasa_historica"].round(6)
    out = out.sort_values(["departamento", "distrito", "anio", "mes"])
    hechos = pd.read_csv(os.path.join(DASH, "gold_riesgo_distrito_mes.csv"))
    assert len(out) == len(hechos), "filas distintas a la tabla de hechos"
    out.to_csv(os.path.join(DASH, "tasa_historica_distrito_mes.csv"), index=False)
    p("tasa_historica_distrito_mes.csv: %d filas (= filas de gold_riesgo_distrito_mes)" % len(out))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", required=True)
    args = ap.parse_args()
    impacto()
    tasa(args.panel)
    with open(EVID, "w", encoding="utf-8") as f:
        f.write("\n".join(LINEAS) + "\n")
    print("\n-> %s" % os.path.relpath(EVID, BASE))
