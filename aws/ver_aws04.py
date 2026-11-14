"""Vistas de terminal para las capturas del paso 3b (sin claves; el ID de cuenta no se muestra).

    python aws/ver_aws04.py jobs       -> jobs sedalib-esc-* con estado, duracion, vCPU-h y GB-h
    python aws/ver_aws04.py replicas   -> s3://<bucket>/escalabilidad/ por escala y tipo, con tamano
"""
import sys

import boto3

REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
APP_ID = "00ga9klqb8i0lq0d"


def jobs():
    emr = boto3.client("emr-serverless", region_name=REGION)
    runs = [r for p in emr.get_paginator("list_job_runs").paginate(applicationId=APP_ID) for r in p["jobRuns"]
            if r.get("name", "").startswith("sedalib-esc-")]
    print("Aplicacion sedalib-spark (%s) - jobs de escalabilidad (paso 3b)\n" % APP_ID)
    print("%-26s %-18s %-9s %8s %8s %8s" % ("job", "id", "estado", "dur(s)", "vCPU-h", "GB-h"))
    print("-" * 82)
    for r in sorted(runs, key=lambda x: x["createdAt"]):
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=r["id"])["jobRun"]
        u = jr.get("totalResourceUtilization", {})
        print("%-26s %-18s %-9s %8s %8.4f %8.4f" % (jr["name"], jr["jobRunId"], jr["state"],
              jr.get("totalExecutionDurationSeconds", "-"), u.get("vCPUHour", 0), u.get("memoryGBHour", 0)))


def replicas():
    s3 = boto3.client("s3", region_name=REGION)
    grupos, tot = {}, [0, 0]
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="escalabilidad/"):
        for o in p.get("Contents", []):
            k = o["Key"]
            tipo = ("replica interrupciones (CSV)" if "sunass_interrupciones.csv/" in k else
                    "processed/ (salida A)" if "/processed/" in k else "raw/ copiado (sin replicar)")
            g = grupos.setdefault((k.split("/")[1], tipo), [0, 0])
            g[0] += 1
            g[1] += o["Size"]
            tot[0] += 1
            tot[1] += o["Size"]
    print("s3://<bucket>/escalabilidad/  (solo rendimiento; no alimenta el modelo)\n")
    print("%-7s %-30s %9s %11s" % ("escala", "contenido", "archivos", "tamano"))
    print("-" * 62)
    for (esc, tipo), (n, b) in sorted(grupos.items(), key=lambda kv: (int(kv[0][0][:-1]), kv[0][1])):
        tam = "%.2f GB" % (b / 2 ** 30) if b >= 2 ** 30 else "%.1f MB" % (b / 2 ** 20)
        print("%-7s %-30s %9d %11s" % (esc, tipo, n, tam))
    print("-" * 62)
    print("%-38s %9d %11s" % ("TOTAL", tot[0], "%.2f GB" % (tot[1] / 2 ** 30)))


if __name__ == "__main__":
    {"jobs": jobs, "replicas": replicas}[sys.argv[1]]()
