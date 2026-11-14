"""AWS paso 3b: escalabilidad en EMR Serverless (replicas 100X / 500X / 1000X).

Uso (desde la raiz del proyecto; no lee ni imprime claves; el ID de cuenta se enmascara):
    python aws/04_escalabilidad_emr.py             # --plan: jobs, orden y costo estimado (no lanza nada)
    python aws/04_escalabilidad_emr.py --aplicar   # lanza los jobs, lista las replicas y escribe evidencias
    python aws/04_escalabilidad_emr.py --borrar    # borra escalabilidad/ con TODAS sus versiones

Orden: generar 100X+500X -> B (subpipeline de src/escalabilidad_spark.py, mismo que en local) 100X+500X
       -> A (src/procesamiento_spark.py sin cambios) 100X y 500X -> 1000X (gen, B, A) solo si 500X OK.
* Replicas: aws/emr_generar_replicas.py, mismo metodo que en local (crossJoin + sufijo en IDINTERRUPCION),
  en s3://<bucket>/escalabilidad/<N>X/raw/. El resto de raw/ se copia dentro de S3, de modo que
  SEDALIB_DATALAKE=s3://<bucket>/escalabilidad/<N>X/ hace correr procesamiento_spark.py sin cambios y
  escribir en escalabilidad/<N>X/processed/. Las replicas NO tocan raw/ ni processed/ reales: no alimentan
  el modelo.
* Un fallo por memoria (OOM) se anota como resultado y NO se reintenta. Otro tipo de fallo detiene todo.
"""
import argparse
import gzip
import json
import os
import re
import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
CUENTA = "856152566912"
APP_ID = "00ga9klqb8i0lq0d"
ROL = "arn:aws:iam::%s:role/sedalib-emr-execution-role" % CUENTA
S3 = "s3://%s/" % BUCKET
P_VCPU, P_GB, P_DISCO = 0.052624, 0.0057785, 0.000111          # us-east-2, API Pricing 2026-11-13
TECHO_H = 4 * (2 * P_VCPU + 8 * P_GB)                            # 4 workers de 2 vCPU / 8 GB
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
ESTADO = os.path.join(EVID, "aws_04_escalabilidad.json")
TXT = os.path.join(EVID, "aws_04_escalabilidad.txt")
LISTADO = os.path.join(EVID, "aws_04_replicas_listado.txt")
LOGS_DIR = os.path.join(EVID, "aws_04_logs")
N_BASE = 45671
ESTIM_MIN = {"gen": {100: 1, 500: 3, 1000: 5}, "B": {100: 2, 500: 4, 1000: 6}, "A": {100: 3, 500: 7, 1000: 12}}
OOM_RE = re.compile(r"OutOfMemoryError|Java heap space|GC overhead|exit code 137|Container killed|OOMKill|"
                    r"exceeds? (the )?memory|ExecutorLostFailure", re.I)

ses = boto3.session.Session(region_name=REGION)
emr, s3 = ses.client("emr-serverless"), ses.client("s3")


def oculto(t):
    return str(t).replace(CUENTA, "<CUENTA>")


def costo(u):
    return u.get("vCPUHour", 0) * P_VCPU + u.get("memoryGBHour", 0) * P_GB + u.get("storageGBHour", 0) * P_DISCO


def leer_log(job_id, tipo):
    clave = "logs/applications/%s/jobs/%s/SPARK_DRIVER/%s.gz" % (APP_ID, job_id, tipo)
    for _ in range(12):
        try:
            return gzip.decompress(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read()).decode("utf-8", "replace")
        except ClientError:
            time.sleep(10)
    return ""


def params(scripts, env):
    conf = {"spark.driver.cores": "2", "spark.driver.memory": "7g", "spark.executor.cores": "2",
            "spark.executor.memory": "7g", "spark.dynamicAllocation.maxExecutors": "3"}
    for k, v in env.items():
        conf["spark.emr-serverless.driverEnv." + k] = v
        conf["spark.executorEnv." + k] = v
    py = ",".join(S3 + "scripts/" + f for f in ["comun.py"] + scripts)
    return "--py-files %s " % py + " ".join("--conf %s=%s" % kv for kv in conf.items())


def lanzar(nombre, script, env, log):
    env = dict({"SEDALIB_SPARK_MASTER": "auto", "SEDALIB_EVID": "/tmp/sedalib_evid/"}, **env)
    jid = emr.start_job_run(
        applicationId=APP_ID, executionRoleArn=ROL, name="sedalib-esc-" + nombre,
        jobDriver={"sparkSubmit": {"entryPoint": S3 + "scripts/emr_lanzador.py", "entryPointArguments": [script],
                                   "sparkSubmitParameters": params([script], env)}},
        configurationOverrides={"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": S3 + "logs/"}}},
        tags={"Proyecto": "SEDALIB", "Curso": "BigData-UPAO", "Uso": "escalabilidad"})["jobRunId"]
    log("%-14s lanzado: %s" % (nombre, jid))
    return recoger(nombre, jid, log)


def recoger(nombre, jid, log):
    """Espera el fin del job y recoge estado, uso real (totalResourceUtilization) y logs."""
    while True:
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=jid)["jobRun"]
        if jr["state"] in ("SUCCESS", "FAILED", "CANCELLED"):
            break
        time.sleep(20)
    for _ in range(12):                    # el uso se publica unos segundos despues del fin del job
        if (jr.get("totalResourceUtilization") or {}).get("vCPUHour"):
            break
        time.sleep(10)
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=jid)["jobRun"]
    out, err = leer_log(jid, "stdout"), ""
    if jr["state"] != "SUCCESS":
        err = leer_log(jid, "stderr")
    for tipo, txt in (("stdout", out), ("stderr", err)):
        if txt:
            with open(os.path.join(LOGS_DIR, "%s_%s.txt" % (nombre, tipo)), "w", encoding="utf-8") as f:
                f.write(oculto(txt))
    u = jr.get("totalResourceUtilization") or {}
    m = re.search(r"\[lanzador\] tiempo del script \S+: ([\d.]+) s", out)
    oom = jr["state"] != "SUCCESS" and bool(OOM_RE.search(err + jr.get("stateDetails", "")))
    r = dict(nombre=nombre, id=jid, estado=jr["state"], oom=oom, dur_s=jr.get("totalExecutionDurationSeconds"),
             script_s=float(m.group(1)) if m else None, vcpu_h=u.get("vCPUHour", 0), gb_h=u.get("memoryGBHour", 0),
             disco_gb_h=u.get("storageGBHour", 0), costo=costo(u), detalle=oculto(jr.get("stateDetails", ""))[:300])
    log("%-14s %s%s en %ss (script %s s) | vCPU-h %.4f | GB-h %.4f | costo %.4f USD"
        % (nombre, jr["state"], " (OOM)" if oom else "", r["dur_s"], r["script_s"], r["vcpu_h"], r["gb_h"], r["costo"]))
    if jr["state"] != "SUCCESS":
        log("   detalle: %s" % r["detalle"])
        for l in [x for x in err.splitlines() if OOM_RE.search(x) or "Error" in x][-6:]:
            log("   " + oculto(l)[:180])
    return r, out


def copiar_raw(n, log):
    """Copia (dentro de S3) todo raw/ salvo interrupciones a escalabilidad/<N>X/raw/."""
    k = 0
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/"):
        for o in p.get("Contents", []):
            if o["Key"].endswith("/") or o["Key"].endswith("sunass_interrupciones.csv"):
                continue
            s3.copy({"Bucket": BUCKET, "Key": o["Key"]}, BUCKET, "escalabilidad/%dX/%s" % (n, o["Key"]))
            k += 1
    log("copiados %d archivos de raw/ a escalabilidad/%dX/raw/ (copia dentro de S3)" % (k, n))


def parse_b(out):
    """{escala: dict} desde las lineas '-> NX | filas=... | T s | ... | spill mem= | spill disco= | estado'."""
    res = {}
    for m in re.finditer(r"->\s+(\d+)X \| filas=\s*(\d+) \|\s+([\d.]+) s \| mem pico contenedor=\s*(\d+) MiB \| "
                         r"spill mem=\s*(\S+) MiB \| spill disco=\s*(\S+) MiB \| (.*)", out):
        res[int(m.group(1))] = dict(filas=int(m.group(2)), t=float(m.group(3)), spill_mem=m.group(5),
                                   spill_disk=m.group(6), estado=m.group(7).strip())
    return res


def plan(log):
    log("Aplicacion sedalib-spark (%s) | rol sedalib-emr-execution-role | s3://<bucket>/escalabilidad/" % APP_ID)
    log("Recursos por job: driver + <=3 executors de 2 vCPU / 8 GB | techo %.3f USD/h" % TECHO_H)
    tot = 0
    for fase, escalas in (("fase 1", (100, 500)), ("fase 2 (solo si 500X OK)", (1000,))):
        log(fase)
        for paso, desc in (("gen", "generar replicas"), ("B", "subpipeline local (escalabilidad_spark.py)"),
                           ("A", "procesamiento_spark.py sin cambios")):
            mins = sum(ESTIM_MIN[paso][n] for n in escalas) + 1      # +1 min de arranque por job
            tot += mins
            log("   %-4s %-44s %-12s ~%2d min  <= %.3f USD" % (paso, desc, "+".join("%dX" % n for n in escalas), mins,
                                                               TECHO_H * mins / 60))
    log("Total ~%d min -> <= %.2f USD de computo (techo)" % (tot, TECHO_H * tot / 60))


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--aplicar", action="store_true")
    g.add_argument("--borrar", action="store_true")
    g.add_argument("--continuar", action="store_true", help="recoge los jobs ya lanzados y corre solo B 1000X")
    a = ap.parse_args()
    if a.borrar:
        return borrar()
    log = print
    if a.continuar:
        return continuar(log)
    if not a.aplicar:
        plan(log)
        log("\nPLAN: no se lanzo nada.")
        return 0

    os.makedirs(LOGS_DIR, exist_ok=True)
    for f in ("emr_lanzador.py", "emr_generar_replicas.py"):
        s3.upload_file(os.path.join(BASE, "aws", f), BUCKET, "scripts/" + f)
    for f in ("comun.py", "escalabilidad_spark.py", "procesamiento_spark.py"):
        s3.upload_file(os.path.join(BASE, "src", f), BUCKET, "scripts/" + f)
    log("scripts subidos a s3://<bucket>/scripts/")
    jobs, b, a_res = [], {}, {}

    def fase(escalas):
        txt = ",".join(str(n) for n in escalas)
        r, out = lanzar("gen_" + "_".join("%dX" % n for n in escalas), "emr_generar_replicas.py",
                        {"SEDALIB_DATALAKE": S3, "ESCALAS_REPLICA": txt}, log)
        jobs.append(r)
        if r["estado"] != "SUCCESS":
            return False
        for n in escalas:
            copiar_raw(n, log)
        r, out = lanzar("B_" + "_".join("%dX" % n for n in escalas), "escalabilidad_spark.py",
                        {"SEDALIB_DATALAKE": S3, "ESCALAS": txt,
                         "SEDALIB_EVID_DESTINO": S3 + "curated/escalabilidad/B_%s/" % txt.replace(",", "_")}, log)
        jobs.append(r)
        b.update(parse_b(out))
        if r["estado"] != "SUCCESS" and not r["oom"]:
            return False
        for n in escalas:
            r, out = lanzar("A_%dX" % n, "procesamiento_spark.py", {"SEDALIB_DATALAKE": S3 + "escalabilidad/%dX/" % n}, log)
            jobs.append(r)
            m = re.search(r"Verificacion de lectura: (\d+) filas", out)
            a_res[n] = dict(r, panel=int(m.group(1)) if m else None)
            if r["estado"] != "SUCCESS" and not r["oom"]:
                return False
        return True

    ok = fase((100, 500))
    ok500 = ok and b.get(500, {}).get("estado") == "OK" and a_res.get(500, {}).get("estado") == "SUCCESS"
    log("500X: B=%s | A=%s -> %s" % (b.get(500, {}).get("estado"), a_res.get(500, {}).get("estado"),
                                      "se ejecuta 1000X" if ok500 else "NO se ejecuta 1000X"))
    if ok500:
        fase((1000,))
    return finalizar(jobs, b, a_res, ok500)


NOTA_A1000 = ("no ejecutado por costo y tiempo; A_100X y A_500X bastan para medir el pipeline real a escala "
              "(decision del usuario: A_100X tardo 8 min y A_500X 31 min; A_1000X se proyectaba en ~80 min)")
YA_LANZADOS = [("gen_100X_500X", "00gaa32sepps900f"), ("B_100X_500X", "00gaa35ftdc0c00f"),
               ("A_100X", "00gaa386e3g2uo0f"), ("A_500X", "00gaa3dj2ubplo0f"), ("gen_1000X", "00gaa3vs0qtf600f")]


def continuar(log):
    """Opcion (b) del usuario: recoge los jobs ya lanzados (uso real y logs) y corre SOLO B 1000X."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    jobs, b, a_res = [], {}, {}
    for nombre, jid in YA_LANZADOS:
        r, out = recoger(nombre, jid, log)
        jobs.append(r)
        if nombre.startswith("B_"):
            b.update(parse_b(out))
        if nombre.startswith("A_"):
            m = re.search(r"Verificacion de lectura: (\d+) filas", out)
            a_res[int(nombre[2:-1])] = dict(r, panel=int(m.group(1)) if m else None)
    if jobs[-1]["estado"] == "SUCCESS":
        r, out = lanzar("B_1000X", "escalabilidad_spark.py",
                        {"SEDALIB_DATALAKE": S3, "ESCALAS": "1000",
                         "SEDALIB_EVID_DESTINO": S3 + "curated/escalabilidad/B_1000/"}, log)
        jobs.append(r)
        b.update(parse_b(out))
    log("A_1000X: " + NOTA_A1000)
    return finalizar(jobs, b, a_res, True, NOTA_A1000)


def finalizar(jobs, b, a_res, ejecuto_1000, nota_a1000=None):
    # listado de replicas (antes de borrar)
    grupos = {}
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="escalabilidad/"):
        for o in p.get("Contents", []):
            k = o["Key"]
            esc = k.split("/")[1]
            tipo = "replica" if "sunass_interrupciones.csv/" in k else ("processed" if "/processed/" in k else "raw copiado")
            g = grupos.setdefault((esc, tipo), [0, 0])
            g[0] += 1
            g[1] += o["Size"]
    with open(LISTADO, "w", encoding="utf-8") as f:
        f.write("Listado de s3://<bucket>/escalabilidad/ antes de borrar (%s)\n" % time.strftime("%Y-%m-%d %H:%M"))
        f.write("%-8s %-12s %9s %12s\n" % ("escala", "tipo", "archivos", "MB"))
        for (esc, tipo), (nf, by) in sorted(grupos.items(), key=lambda kv: (int(kv[0][0][:-1]), kv[0][1])):
            f.write("%-8s %-12s %9d %12.2f\n" % (esc, tipo, nf, by / 1048576))
        f.write("TOTAL %26d %12.2f\n" % (sum(v[0] for v in grupos.values()), sum(v[1] for v in grupos.values()) / 1048576))
    json.dump(dict(jobs=jobs, B=b, A={str(k): v for k, v in a_res.items()},
                   replicas={"%s|%s" % k: v for k, v in grupos.items()}, ejecuto_1000=ejecuto_1000,
                   nota_A1000=nota_a1000),
              open(ESTADO, "w"), indent=1)
    escribir_txt()
    print(open(TXT, encoding="utf-8").read())
    return 0


def escribir_txt():
    st = json.load(open(ESTADO))
    loc = {r["escala"]: r for r in json.load(open(os.path.join(EVID, "escalabilidad_spark.json")))}
    L = []
    w = L.append
    w("=" * 92)
    w(" AWS paso 3b - ESCALABILIDAD EN EMR SERVERLESS (sedalib-spark, emr-7.9.0 / Spark 3.5.5, us-east-2)")
    w("=" * 92)
    w("Replicas = solo para medir rendimiento. Estan en s3://<bucket>/escalabilidad/ (separado de raw/ y")
    w("processed/ reales) y NO alimentan el modelo. Metodo identico al local: crossJoin(spark.range(N)) + sufijo")
    w("en IDINTERRUPCION sobre las %d filas reales de interrupciones." % N_BASE)
    w("Recursos AWS por job: driver + hasta 3 executors de 2 vCPU / 8 GB (8 vCPU / 32 GB en total).")
    w("Recursos locales: Spark local[16], heap del driver 1 GiB (Docker).")
    w("")
    w("1. JOBS (get-job-run; costo real = vCPU-h x %.6f + GB-h x %.7f + disco GB-h x %.6f)" % (P_VCPU, P_GB, P_DISCO))
    w("%-14s %-18s %-12s %7s %9s %8s %8s %9s" % ("job", "id", "estado", "dur(s)", "script(s)", "vCPU-h", "GB-h", "costo USD"))
    for r in st["jobs"]:
        w("%-14s %-18s %-12s %7s %9s %8.4f %8.4f %9.4f" % (r["nombre"], r["id"], r["estado"] + (" OOM" if r["oom"] else ""),
          r["dur_s"], "%.1f" % r["script_s"] if r["script_s"] else "-", r["vcpu_h"], r["gb_h"], r["costo"]))
    w("%-14s %-18s %-12s %7d %9s %8.4f %8.4f %9.4f" % ("TOTAL", "", "", sum(r["dur_s"] or 0 for r in st["jobs"]), "",
      sum(r["vcpu_h"] for r in st["jobs"]), sum(r["gb_h"] for r in st["jobs"]), sum(r["costo"] for r in st["jobs"])))
    w("")
    w("2. TABLA PRINCIPAL - mismo subpipeline (B) local vs AWS: dropDuplicates -> normalizar -> JOIN clima,")
    w("   peligros e infraestructura -> groupBy distrito-mes -> write(noop). Tiempo medido dentro del script.")
    w("%-6s %12s | %-26s | %-26s | %9s %9s" % ("escala", "filas", "LOCAL (Docker)", "AWS (EMR Serverless)",
                                               "spill MiB", "disco MiB"))
    for n in (100, 500, 1000):
        lr, ar = loc.get(n), st["B"].get(str(n))
        if lr:
            ls = ("%.2f s (OK, spill)" % lr["tiempo_s"]) if lr["estado"] == "OK" else                 ("OOM heap a los %.2f s" % lr["tiempo_s"])
        else:
            ls = "no se ejecuto"
        if ar:
            as_ = ("%.2f s (OK)" % ar["t"]) if ar["estado"] == "OK" else ("%s a los %.2f s" % (ar["estado"][:12], ar["t"]))
        else:
            as_ = "no se ejecuto"
        w("%-6s %12s | %-26s | %-26s | %9s %9s" % ("%dX" % n, "{:,}".format(N_BASE * n), ls, as_,
          ar["spill_mem"] if ar else "-", ar["spill_disk"] if ar else "-"))
    w("   (spill AWS: API REST del driver; 'None' = la API no respondio dentro de EMR)")
    w("")
    w("3. SERIE APARTE (A) - src/procesamiento_spark.py SIN CAMBIOS sobre cada replica (pipeline completo:")
    w("   limpieza, clima, peligros, infraestructura, panel distrito-mes con rezagos). Sin equivalente local.")
    w("%-6s %12s %-12s %10s %10s %12s %9s" % ("escala", "filas ent.", "estado", "script(s)", "job(s)", "panel filas",
                                             "costo USD"))
    for n in (100, 500, 1000):
        r = st["A"].get(str(n))
        if r:
            w("%-6s %12s %-12s %10s %10s %12s %9.4f" % ("%dX" % n, "{:,}".format(N_BASE * n),
              r["estado"] + (" OOM" if r["oom"] else ""), "%.1f" % r["script_s"] if r["script_s"] else "-",
              r["dur_s"], r["panel"], r["costo"]))
        else:
            if n == 1000 and st.get("nota_A1000"):
                w("%-6s %12s %s" % ("%dX" % n, "{:,}".format(N_BASE * n), "NO EJECUTADO (ver nota al final)"))
            else:
                w("%-6s %12s %s" % ("%dX" % n, "{:,}".format(N_BASE * n), "no se ejecuto"))
    w("   El panel sigue teniendo 19 380 filas (grilla distrito-mes); lo que crece es N_INTERRUPCIONES.")
    w("")
    w("4. REPLICAS EN S3 (antes de borrar; detalle en evidencias/aws_04_replicas_listado.txt)")
    for k, (nf, by) in sorted(st["replicas"].items(), key=lambda kv: (int(kv[0].split("|")[0][:-1]), kv[0])):
        w("   %-24s %6d archivos %10.2f MB" % (k.replace("|", " "), nf, by / 1048576))
    w("")
    if st.get("nota_A1000"):
        w("1000X: 500X termino bien en B y A -> se genero 1000X y se corrio SOLO B 1000X (opcion b del usuario);")
        w("       A 1000X: no ejecutado por costo y tiempo; A_100X y A_500X bastan para medir el pipeline")
        w("       real a escala (A_100X tardo 8 min y A_500X 31 min; A_1000X se proyectaba en ~80 min).")
    else:
        w("1000X: %s" % ("ejecutado (500X termino bien en B y A)" if st["ejecuto_1000"] else
                         "NO ejecutado (500X no termino bien en B y A)"))
    bb = {int(k): v for k, v in st["B"].items() if v["estado"] == "OK"}
    aa = {int(k): v for k, v in st["A"].items() if v["estado"] == "SUCCESS"}
    if 100 in bb and 1000 in bb:
        w("")
        w("5. LECTURA DE RESULTADOS")
        w("* 100X: local %.2f s vs AWS %.2f s. Local es mas rapido a esta escala: 16 nucleos en una sola JVM y"
          % (loc[100]["tiempo_s"], bb[100]["t"]))
        w("  datos en memoria; EMR tiene 6 nucleos de executors y lee de S3 por red. Aun asi local ya hacia spill"
          " (%.0f MiB)." % loc[100]["spill_mem_mib"])
        if 500 in bb:
            w("* 500X: local cae por OOM (Java heap space, heap 1 GiB) a los %.2f s; AWS termina en %.2f s"
              % (loc[500]["tiempo_s"], bb[500]["t"]))
            w("  repartiendo la memoria entre driver y executors (spill %s MiB, sin fallo). Punto de quiebre local = 500X."
              % bb[500]["spill_mem"])
        w("* Escalado sublineal en AWS (B): de 100X a 1000X los datos crecen 10x y el tiempo %.1fx (%.2f -> %.2f s)."
          % (bb[1000]["t"] / bb[100]["t"], bb[100]["t"], bb[1000]["t"]))
        if 100 in aa and 500 in aa:
            w("* A (pipeline completo): de 100X a 500X los datos crecen 5x y el tiempo %.1fx (%.0f -> %.0f s); es"
              % (aa[500]["script_s"] / aa[100]["script_s"], aa[100]["script_s"], aa[500]["script_s"]))
            w("  mucho mas pesado que B (mas fuentes, ventanas para rezagos, varias escrituras parquet a S3).")
        w("* Costo total del paso 3b: %.4f USD (A_500X = %.0f %% del total)."
          % (sum(r["costo"] for r in st["jobs"]),
             100 * next((r["costo"] for r in st["jobs"] if r["nombre"] == "A_500X"), 0) /
             max(sum(r["costo"] for r in st["jobs"]), 1e-9)))
    if "borrado" in st:
        w("")
        w("6. BORRADO de s3://<bucket>/escalabilidad/: %s" % st["borrado"])
    open(TXT, "w", encoding="utf-8").write("\n".join(L) + "\n")


def borrar():
    n_v = n_m = 0
    for p in s3.get_paginator("list_object_versions").paginate(Bucket=BUCKET, Prefix="escalabilidad/"):
        objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in p.get("Versions", [])]
        n_v += len(objs)
        marks = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in p.get("DeleteMarkers", [])]
        n_m += len(marks)
        todo = objs + marks
        for i in range(0, len(todo), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": todo[i:i + 1000], "Quiet": True})
    quedan = sum(len(p.get("Versions", [])) + len(p.get("DeleteMarkers", []))
                 for p in s3.get_paginator("list_object_versions").paginate(Bucket=BUCKET, Prefix="escalabilidad/"))
    st = json.load(open(ESTADO))
    st["borrado"] = ("%s | %d versiones de objetos y %d marcadores de borrado eliminados; quedan %d"
                     % (time.strftime("%Y-%m-%d %H:%M"), n_v, n_m, quedan))
    json.dump(st, open(ESTADO, "w"), indent=1)
    escribir_txt()
    print(st["borrado"])
    return 0 if quedan == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
