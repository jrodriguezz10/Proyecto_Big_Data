"""5b. Mismo pipeline de escalabilidad que escalabilidad_spark.py, pero en pandas (un solo proceso).

Se ejecuta en un contenedor con limite de memoria para que el fallo por memoria sea real
y no afecte al host:
  docker run --rm -m 4g --memory-swap 4g -v "${PWD}:/app" proyecto2-app python /app/src/pandas_escalabilidad.py

Cada escala corre en un proceso hijo: si el kernel lo mata por memoria (OOM killer del
cgroup, exitcode -9) o pandas lanza MemoryError, se registra y se detiene.
Lee los mismos archivos raw (copia local identica a la subida a HDFS).
"""
import json
import multiprocessing as mp
import os
import resource
import time

import numpy as np
import pandas as pd

RAW = "/app/data/raw/"
SALIDA = "/app/evidencias/escalabilidad_pandas.json"
ESCALAS = [int(x) for x in os.environ.get("ESCALAS", "1,10,100,500,1000").split(",")]
KEY_D = ["DEPTO_N", "DIST_N"]
TABLA = str.maketrans("ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ�_", "AEIOUAEIOUAEIOUNN ")


def norm(s):
    s = s.astype("string").str.strip().str.upper().str.translate(TABLA)
    return s.str.replace(r"\s+", " ", regex=True).str.strip()


def limite_cgroup():
    try:
        v = open("/sys/fs/cgroup/memory.max").read().strip()
        return "sin limite" if v == "max" else "%d MiB" % (int(v) // 2 ** 20)
    except OSError:
        return "?"


def dimensiones():
    c = pd.read_csv(RAW + "clima/senamhi_clima.csv", dtype={"FECHA": str, "HORA": str})
    c = c.drop_duplicates()
    c["PP"] = c["PP"].fillna(0.0)
    c["DEPTO_N"], c["DIST_N"] = norm(c["DEPARTAMENTO"]), norm(c["DISTRITO"])
    dia = c.groupby(["DEPTO_N", "DIST_N", "ESTACION", "FECHA"], as_index=False)["PP"].sum()
    dia["ANIO"], dia["MES"] = dia["FECHA"].str[:4].astype(int), dia["FECHA"].str[4:6].astype(int)
    mes = dia.groupby(["DEPTO_N", "DIST_N", "ESTACION", "ANIO", "MES"], as_index=False)["PP"].sum()
    clima = mes.groupby(["DEPTO_N", "ANIO", "MES"], as_index=False)["PP"].mean().rename(columns={"PP": "PP_TOTAL"})
    p = pd.read_csv(RAW + "geologia/ingemmet_peligros.csv", sep=";", encoding="latin-1", dtype=str,
                    usecols=["NM_DPTO", "NM_DIST"])
    p["DEPTO_N"], p["DIST_N"] = norm(p["NM_DPTO"]), norm(p["NM_DIST"])
    pel = p.dropna(subset=["DIST_N"]).groupby(KEY_D).size().rename("N_PELIGROS").reset_index()
    a = pd.read_csv(RAW + "infraestructura/datass_antiguedad.csv", sep=";", dtype=str)
    a["DEPTO_N"], a["DIST_N"] = norm(a["DEPARTAMENTO"]), norm(a["DISTRITO"])
    a["MAS20"] = norm(a["VALOR"]).str.contains("MAS DE 20").astype(float)
    infra = a.groupby(KEY_D)["MAS20"].mean().rename("PCT_MAS_20").reset_index()
    return clima, pel, infra


def pipeline(n, base, clima, pel, infra, cola):
    try:
        t0 = time.perf_counter()
        rep = pd.concat([base] * n, ignore_index=True)
        rep["IDINTERRUPCION"] = rep["IDINTERRUPCION"] + "_" + np.repeat(np.arange(n), len(base)).astype(str)
        rep = rep.drop_duplicates()
        rep["DEPTO_N"], rep["DIST_N"] = norm(rep["DEPARTAMENTO"]), norm(rep["DISTRITO"])
        rep = rep.drop_duplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"])
        f = pd.to_datetime(rep["FECHAINICIO"], format="%Y%m%d", errors="coerce")
        rep = rep[f.notna() & rep["DIST_N"].notna()]
        f = f[rep.index]
        rep = rep.assign(ANIO=f.dt.year, MES=f.dt.month)
        m = (rep.merge(clima, on=["DEPTO_N", "ANIO", "MES"], how="left")
             .merge(pel, on=KEY_D, how="left").merge(infra, on=KEY_D, how="left"))
        out = m.groupby(KEY_D + ["ANIO", "MES"]).agg(N_INT=("IDINTERRUPCION", "size"), PP_TOTAL=("PP_TOTAL", "mean"),
                                                     N_PELIGROS=("N_PELIGROS", "max"), PCT_MAS_20=("PCT_MAS_20", "max"))
        t = time.perf_counter() - t0
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KiB -> MiB
        cola.put({"estado": "OK", "tiempo_s": round(t, 2), "rss_pico_mib": round(rss), "filas_resultado": len(out)})
    except MemoryError:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        cola.put({"estado": "MemoryError", "rss_pico_mib": round(rss)})


if __name__ == "__main__":
    print("=" * 78)
    print(" 5b. PRUEBA DE ESCALABILIDAD - PANDAS %s | limite de memoria del contenedor: %s"
          % (pd.__version__, limite_cgroup()))
    print("=" * 78)
    clima, pel, infra = dimensiones()
    base = pd.read_csv(RAW + "operaciones/sunass_interrupciones.csv", dtype=str, encoding="utf-8-sig")
    print("filas base: %d | dimensiones: clima=%d peligros=%d infra=%d" % (len(base), len(clima), len(pel), len(infra)))
    ctx = mp.get_context("fork")
    resultados = []
    for n in ESCALAS:
        cola = ctx.Queue()
        t0 = time.perf_counter()
        proc = ctx.Process(target=pipeline, args=(n, base, clima, pel, infra, cola))
        proc.start()
        proc.join()
        r = cola.get() if not cola.empty() else {}
        if proc.exitcode == -9:
            r = {"estado": "OOM (proceso terminado por el kernel, SIGKILL)", "tiempo_hasta_fallo_s": round(time.perf_counter() - t0, 2)}
        elif proc.exitcode != 0 and not r:
            r = {"estado": "ERROR exitcode=%s" % proc.exitcode}
        r.update(escala=n, filas=len(base) * n)
        resultados.append(r)
        print("-> %5dX | filas=%11d | %s" % (n, r["filas"], r), flush=True)
        if r["estado"] != "OK":
            print("   pandas falla en %dX; se detiene." % n)
            break
    with open(SALIDA, "w") as f:
        json.dump({"limite_memoria_contenedor": limite_cgroup(), "pandas": pd.__version__, "resultados": resultados}, f, indent=1)
    print("Resultados guardados en %s" % SALIDA)
