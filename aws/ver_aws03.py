"""Vistas de terminal para las capturas del paso 3a (no lee ni imprime claves).

    python aws/ver_aws03.py jobs      -> jobs sedalib-* de la aplicacion: estado y duracion (get-job-run)
    python aws/ver_aws03.py salidas   -> archivos en processed/ y curated/ (por dataset)
    python aws/ver_aws03.py metricas  -> comparacion local vs AWS (seccion g de evidencias/aws_03_pipeline.txt)
"""
import os
import sys
from collections import OrderedDict

import boto3

REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
APP_ID = "00ga9klqb8i0lq0d"
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def jobs():
    emr = boto3.client("emr-serverless", region_name=REGION)
    runs = [r for p in emr.get_paginator("list_job_runs").paginate(applicationId=APP_ID) for r in p["jobRuns"]
            if r.get("name", "").startswith("sedalib-")]
    print("Aplicacion sedalib-spark (%s) - jobs del pipeline\n" % APP_ID)
    print("%-30s %-20s %-9s %8s %8s %8s" % ("job", "id", "estado", "dur(s)", "vCPU-h", "GB-h"))
    print("-" * 88)
    for r in sorted(runs, key=lambda x: x["createdAt"]):
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=r["id"])["jobRun"]
        u = jr.get("totalResourceUtilization", {})
        print("%-30s %-20s %-9s %8s %8.4f %8.4f" % (jr["name"], jr["jobRunId"], jr["state"],
              jr.get("totalExecutionDurationSeconds", "-"), u.get("vCPUHour", 0), u.get("memoryGBHour", 0)))


def salidas():
    s3 = boto3.client("s3", region_name=REGION)
    for pref in ("processed/", "curated/"):
        grupos = OrderedDict()
        for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=pref):
            for o in p.get("Contents", []):
                if o["Key"].endswith("/"):
                    continue
                partes = o["Key"].split("/")
                clave = "/".join(partes[:3]) if pref == "curated/" else "/".join(partes[:2])
                g = grupos.setdefault(clave, [0, 0, set()])
                g[0] += 1
                g[1] += o["Size"]
                g[2].update(x for x in partes if x.startswith("ANIO="))
        print("s3://%s/%s" % (BUCKET, pref))
        for k, (n, b, anios) in grupos.items():
            a = sorted(int(x.split("=")[1]) for x in anios)
            extra = " | ANIO=%d..%d (%d particiones)" % (a[0], a[-1], len(a)) if a else ""
            print("   %-46s %3d archivos %7.2f MB%s" % (k, n, b / 1048576, extra))
        if pref == "curated/":
            for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=pref):
                for o in p.get("Contents", []):
                    if not o["Key"].endswith("/"):
                        print("      %-55s %9.1f KB" % (o["Key"], o["Size"] / 1024))
        print()


def metricas():
    txt = open(os.path.join(BASE, "evidencias", "aws_03_pipeline.txt"), encoding="utf-8").read().splitlines()
    i_f = [i for i, l in enumerate(txt) if l.startswith(" f. SILVER")][0]
    i_g = [i for i, l in enumerate(txt) if l.startswith(" g. METRICAS")][0]
    i_h = [i for i, l in enumerate(txt) if l.startswith(" h. POR QUE")][0]
    print(txt[i_f + 2])                                    # filas silver
    print(txt[i_g])
    for l in txt[i_g + 2:i_h - 1]:
        if l.strip():
            print(l)
    print("Spark: local 3.2.1 (Docker) | AWS 3.5.5-amzn-0 (EMR 7.9.0). Identicos: ingenua, LR, DT-M1.")
    print("RF/GBT varian por muestreo de cuantiles/bootstrap segun particionado y version; dAUC-PR vs")
    print("ingenua de AWS dentro de los IC 95 % locales en los 16 casos (detalle: seccion h de la evidencia).")


if __name__ == "__main__":
    {"jobs": jobs, "salidas": salidas, "metricas": metricas}[sys.argv[1]]()
