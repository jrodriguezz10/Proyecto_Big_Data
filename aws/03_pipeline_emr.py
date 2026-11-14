"""AWS paso 3a: el MISMO pipeline de src/ en EMR Serverless (prueba de sustitucion, seccion 2.3).

Uso (desde la raiz del proyecto; no lee ni imprime claves):
    python aws/03_pipeline_emr.py             # --plan (por defecto): jobs, orden, recursos y costo estimado
    python aws/03_pipeline_emr.py --aplicar   # sube scripts/, lanza los 4 jobs en orden y escribe
                                              # evidencias/aws_03_pipeline.txt + evidencias/aws_03_logs/

Jobs (en orden; si uno falla, se detiene): procesamiento -> calidad -> integracion clima -> modelo.
Mismo codigo que en Docker: solo cambian SEDALIB_DATALAKE (s3://bucket/), SEDALIB_SPARK_MASTER=auto
y SEDALIB_EVID (salidas CSV del modelo, que aws/emr_lanzador.py copia a curated/).
Recursos por job: driver y executors de 2 vCPU / 8 GB (7g de heap + overhead), maximo 3 executors.
"""
import argparse
import gzip
import os
import re
import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
APP_ID = "00ga9klqb8i0lq0d"
ROL = "arn:aws:iam::856152566912:role/sedalib-emr-execution-role"
S3 = "s3://%s/" % BUCKET
SCRIPTS = "scripts/"
LOGS = "logs/"
# Precios EMR Serverless us-east-2 (x86), API AWS Pricing, consultados el 2026-11-13
P_VCPU, P_GB, P_DISCO = 0.052624, 0.0057785, 0.000111
JOBS = [("01_procesamiento", "procesamiento_spark.py", 6),
        ("02_calidad", "calidad_spark.py", 3),
        ("03_integracion_clima", "integracion_clima.py", 3),
        ("04_modelo_definitivo", "modelo_definitivo.py", 25)]       # (nombre, script, minutos estimados)
WORKERS_MAX, VCPU_W, GB_W = 4, 2, 8                                 # driver + 3 executors
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
LOCAL_04 = os.path.join(EVID, "04_modelo_definitivo.txt")


class Tee:
    def __init__(self, ruta):
        self.f = open(ruta, "w", encoding="utf-8") if ruta else None

    def __call__(self, *a):
        linea = " ".join(str(x) for x in a)
        print(linea, flush=True)
        if self.f:
            self.f.write(linea + "\n")
            self.f.flush()


def titulo(log, t):
    log("\n" + "=" * 78)
    log(" " + t)
    log("=" * 78)


def spark_params(script, job):
    env = {"SEDALIB_DATALAKE": S3, "SEDALIB_SPARK_MASTER": "auto", "SEDALIB_EVID": "/tmp/sedalib_evid/",
           "SEDALIB_EVID_DESTINO": S3 + "curated/evidencias/%s/" % job}
    conf = {"spark.driver.cores": "2", "spark.driver.memory": "7g",
            "spark.executor.cores": "2", "spark.executor.memory": "7g",
            "spark.dynamicAllocation.maxExecutors": "3"}
    for k, v in env.items():
        conf["spark.emr-serverless.driverEnv." + k] = v
        conf["spark.executorEnv." + k] = v
    py = ",".join(S3 + SCRIPTS + f for f in ("comun.py", script))
    return "--py-files %s " % py + " ".join("--conf %s=%s" % kv for kv in conf.items())


def costo(u):
    return u.get("vCPUHour", 0) * P_VCPU + u.get("memoryGBHour", 0) * P_GB + u.get("storageGBHour", 0) * P_DISCO


def leer_log(s3, job_id, tipo="stdout"):
    clave = "%sapplications/%s/jobs/%s/SPARK_DRIVER/%s.gz" % (LOGS, APP_ID, job_id, tipo)
    for _ in range(12):                                   # los logs llegan a S3 unos segundos despues
        try:
            return gzip.decompress(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read()).decode("utf-8", "replace")
        except ClientError:
            time.sleep(10)
    return ""


def filas_metricas(texto):
    """{(ambito, var, modelo): (auc_pr, auc_roc, prec, recall)} de la seccion 4.4 del modelo."""
    out, ambito, en_44 = {}, None, False
    for l in texto.splitlines():
        if "4.4 HOLDOUT" in l:
            en_44 = True
        if not en_44:
            continue
        m = re.match(r"--- Ambito: (\S+)", l)
        if m:
            ambito = m.group(1)
        m = re.match(r"(M[12])\s+(Regla ingenua|LogisticRegression|DecisionTree|RandomForest|GBT)\s+"
                     r"([\d.]+)\s+([\d.]+)\s+[\d.]+\s+([\d.]+)\s+([\d.]+)", l)
        if m and ambito:
            out[(ambito, m.group(1), m.group(2))] = tuple(float(m.group(i)) for i in range(3, 7))
        if l.startswith("La Libertad - holdout por distrito"):
            break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aplicar", action="store_true")
    aplicar = ap.parse_args().aplicar
    log = Tee(os.path.join(EVID, "aws_03_pipeline.txt") if aplicar else None)
    ses = boto3.session.Session(region_name=REGION)
    emr, s3 = ses.client("emr-serverless"), ses.client("s3")

    titulo(log, "a. CONTEXTO - modo %s" % ("APLICAR" if aplicar else "PLAN (no se lanza nada)"))
    ident = ses.client("sts").get_caller_identity()
    app = emr.get_application(applicationId=APP_ID)["application"]
    log("Cuenta %s | %s" % (ident["Account"], ident["Arn"]))
    log("Aplicacion %s (%s) | %s | estado %s | max %s / %s" % (app["name"], APP_ID, app["releaseLabel"],
        app["state"], app["maximumCapacity"]["cpu"], app["maximumCapacity"]["memory"]))
    log("Rol: %s | datos: %sraw/ -> processed/, curated/ | logs: %s%s" % (ROL, S3, S3, LOGS))
    log("Recursos por job: driver 2 vCPU/8 GB + hasta 3 executors 2 vCPU/8 GB (spark.*.memory=7g + overhead)")
    log("Precios us-east-2: %.6f USD/vCPU-h, %.7f USD/GB-h, %.6f USD/GB-h disco (>20 GB/worker)"
        % (P_VCPU, P_GB, P_DISCO))

    titulo(log, "b. JOBS (en orden; se detiene si uno falla)")
    techo_h = WORKERS_MAX * (VCPU_W * P_VCPU + GB_W * P_GB)
    total = 0.0
    for nombre, script, minutos in JOBS:
        c = techo_h * minutos / 60
        total += c
        log("%-22s %-24s ~%2d min  techo %.3f USD (4 workers todo el tiempo)" % (nombre, script, minutos, c))
    log("Techo estimado: %.3f USD por hora con 4 workers | pipeline ~%d min -> <= %.2f USD"
        % (techo_h, sum(j[2] for j in JOBS), total))
    if not aplicar:
        log("\nPLAN: no se subio ni lanzo nada. Ejecutar con --aplicar tras confirmar.")
        return 0

    titulo(log, "c. SUBIDA DE SCRIPTS a %s%s" % (S3, SCRIPTS))
    for ruta in [os.path.join(BASE, "src", "comun.py"), os.path.join(BASE, "aws", "emr_lanzador.py")] + \
            [os.path.join(BASE, "src", j[1]) for j in JOBS]:
        s3.upload_file(ruta, BUCKET, SCRIPTS + os.path.basename(ruta))
        log("subido %s%s" % (SCRIPTS, os.path.basename(ruta)))

    titulo(log, "d. EJECUCION")
    os.makedirs(os.path.join(EVID, "aws_03_logs"), exist_ok=True)
    resumen, stdout = [], {}
    for nombre, script, minutos in JOBS:
        t0 = time.time()
        jid = emr.start_job_run(
            applicationId=APP_ID, executionRoleArn=ROL, name="sedalib-" + nombre,
            jobDriver={"sparkSubmit": {"entryPoint": S3 + SCRIPTS + "emr_lanzador.py",
                                       "entryPointArguments": [script],
                                       "sparkSubmitParameters": spark_params(script, nombre)}},
            configurationOverrides={"monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": S3 + LOGS}}},
            tags={"Proyecto": "SEDALIB", "Curso": "BigData-UPAO"})["jobRunId"]
        log("%s lanzado: %s" % (nombre, jid))
        while True:
            jr = emr.get_job_run(applicationId=APP_ID, jobRunId=jid)["jobRun"]
            if jr["state"] in ("SUCCESS", "FAILED", "CANCELLED"):
                break
            time.sleep(20)
        u = jr.get("totalResourceUtilization") or {}                 # uso real (lo pedido)
        ub = jr.get("billedResourceUtilization") or {}               # facturado (min. 1 min/worker, 20 GB disco gratis)
        dur = jr.get("totalExecutionDurationSeconds") or int(time.time() - t0)
        resumen.append((nombre, jid, jr["state"], dur, u, ub, techo_h * minutos / 60))
        stdout[nombre] = leer_log(s3, jid)
        with open(os.path.join(EVID, "aws_03_logs", nombre + "_stdout.txt"), "w", encoding="utf-8") as f:
            f.write(stdout[nombre])
        log("%s %s en %ds | vCPU-h %.4f | GB-h %.4f | costo %.4f USD"
            % (nombre, jr["state"], dur, u.get("vCPUHour", 0), u.get("memoryGBHour", 0), costo(u)))
        if jr["state"] != "SUCCESS":
            err = leer_log(s3, jid, "stderr")
            with open(os.path.join(EVID, "aws_03_logs", nombre + "_stderr.txt"), "w", encoding="utf-8") as f:
                f.write(err)
            log("FALLO: %s | ultimas lineas de stderr:" % jr.get("stateDetails", ""))
            for l in [x for x in err.splitlines() if x.strip()][-15:]:
                log("   " + l[:200])
            break

    titulo(log, "e. RESUMEN POR JOB (get-job-run: totalResourceUtilization = uso real)")
    log("costo real = vCPU-h x %.6f + GB-h x %.7f + disco GB-h x %.6f | facturado = mismo calculo con"
        " billedResourceUtilization | estimado = techo del plan" % (P_VCPU, P_GB, P_DISCO))
    log("%-22s %-20s %-8s %7s %8s %8s %8s %9s %9s %9s" % ("job", "id", "estado", "dur(s)", "vCPU-h", "GB-h",
        "discoGB-h", "real USD", "fact USD", "estim USD"))
    for nombre, jid, st, dur, u, ub, est in resumen:
        log("%-22s %-20s %-8s %7d %8.4f %8.4f %8.4f %9.4f %9.4f %9.4f" % (nombre, jid, st, dur,
            u.get("vCPUHour", 0), u.get("memoryGBHour", 0), u.get("storageGBHour", 0), costo(u), costo(ub), est))
    log("%-51s %7d %8.4f %8.4f %8.4f %9.4f %9.4f %9.4f" % ("TOTAL", sum(r[3] for r in resumen),
        sum(r[4].get("vCPUHour", 0) for r in resumen), sum(r[4].get("memoryGBHour", 0) for r in resumen),
        sum(r[4].get("storageGBHour", 0) for r in resumen), sum(costo(r[4]) for r in resumen),
        sum(costo(r[5]) for r in resumen), sum(r[6] for r in resumen)))

    titulo(log, "f. SILVER (processed/master_panel.parquet)")
    m = re.search(r"Verificacion de lectura: (\d+) filas", stdout.get("01_procesamiento", ""))
    filas = int(m.group(1)) if m else None
    log("Filas silver en S3: %s | esperado 19380 | %s" % (filas, "OK" if filas == 19380 else "REVISAR"))
    m = re.search(r"Escrito .*master_panel_clima.parquet \| verificacion de lectura: (\d+) filas",
                  stdout.get("03_integracion_clima", ""))
    log("Filas master_panel_clima en S3: %s" % (m.group(1) if m else None))

    titulo(log, "g. METRICAS DEL MODELO: AWS (EMR Serverless) vs local (Docker, 04_modelo_definitivo.txt)")
    aws = filas_metricas(stdout.get("04_modelo_definitivo", ""))
    loc = filas_metricas(open(LOCAL_04, encoding="utf-8").read())
    sel = re.search(r"SELECCIONADO.*?: (.*)", stdout.get("04_modelo_definitivo", ""))
    log("Seleccionado en AWS: %s" % (sel.group(1) if sel else "-"))
    log("%-12s %-3s %-20s %9s %9s %9s | %9s %9s" % ("ambito", "var", "modelo", "AUC-PR loc", "AUC-PR aws",
                                                     "dif", "rec loc", "rec aws"))
    for k in sorted(loc):
        a = aws.get(k)
        if a:
            log("%-12s %-3s %-20s %9.4f %9.4f %+9.4f | %9.4f %9.4f" % (k[0], k[1], k[2], loc[k][0], a[0],
                                                                    a[0] - loc[k][0], loc[k][3], a[3]))
    g = aws.get(("NACIONAL", "M1", "GBT"))
    log("\nM1-GBT nacional AUC-PR: local 0.7293 | AWS %s" % ("%.4f" % g[0] if g else "-"))
    return 0 if all(r[2] == "SUCCESS" for r in resumen) and len(resumen) == len(JOBS) else 1


if __name__ == "__main__":
    sys.exit(main())
