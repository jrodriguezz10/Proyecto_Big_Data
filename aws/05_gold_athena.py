"""AWS paso 4: capa gold en EMR Serverless, catalogo Glue (DDL desde Athena) y consultas Athena.

Uso (desde la raiz del proyecto; no lee ni imprime claves; el bucket y el ID de cuenta se enmascaran):
    python aws/05_gold_athena.py --gold      # sube scripts + ubigeo INEI, lanza src/gold.py, evidencia aws_05_gold.txt
    python aws/05_gold_athena.py --athena    # CREATE DATABASE/EXTERNAL TABLE + 3 consultas -> aws_05_athena.txt
    python aws/05_gold_athena.py --exportar  # descarga curated/gold/ a dashboard/data/*.csv

* Glue Data Catalog: base sedalib_gold, tablas creadas con CREATE EXTERNAL TABLE desde Athena (sin crawler).
  Las columnas del DDL se leen del esquema Parquet real (pyarrow), no se escriben a mano.
* Resultados de Athena en s3://<bucket>/logs/athena/ (indicado en cada consulta; el workgroup no se modifica).
* Si Glue o Athena devuelven AccessDenied o un bloqueo de plan, se detiene y muestra el error exacto.
"""
import argparse
import io
import json
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
CUENTA = "856152566912"
APP_ID = "00ga9klqb8i0lq0d"
ROL = "arn:aws:iam::%s:role/sedalib-emr-execution-role" % CUENTA
S3 = "s3://%s/" % BUCKET
P_VCPU, P_GB, P_DISCO = 0.052624, 0.0057785, 0.000111          # us-east-2, API Pricing 2026-11-13
P_ATHENA_TB = 5.0                                               # USD por TB escaneado, minimo 10 MB por consulta
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
LOGS_DIR = os.path.join(EVID, "aws_05_logs")
ESTADO = os.path.join(EVID, "aws_05_estado.json")
UBIGEO_LOCAL = os.path.join(BASE, "data", "raw", "referencia", "ubigeo_inei_2022.csv")
UBIGEO_URL = "https://www.inei.gob.pe/media/DATOS_ABIERTOS/UBIGEOS/UBIGEOS_2022_1891_distritos.zip"
UBIGEO_PAGINA = ("https://www.datosabiertos.gob.pe/dataset/ubigeos-c%C3%B3digos-de-ubicaci%C3%B3n-geogr%C3%A1fica-"
                 "instituto-nacional-de-estad%C3%ADstica-e-inform%C3%A1tica-inei")
UBIGEO_FECHA = "2026-11-14"
DB = "sedalib_gold"
GOLD_PREFIX = "curated/gold/"
TABLAS = ["gold_riesgo_distrito_mes", "dim_distrito", "dim_tiempo", "gold_kpi_mensual", "gold_comparacion_modelos"]
ATHENA_OUT = S3 + "logs/athena/"

ses = boto3.session.Session(region_name=REGION)
emr, s3, athena = ses.client("emr-serverless"), ses.client("s3"), ses.client("athena")


def oculto(t):
    return str(t).replace(BUCKET, "<bucket>").replace(CUENTA, "<CUENTA>")


def costo(u):
    return u.get("vCPUHour", 0) * P_VCPU + u.get("memoryGBHour", 0) * P_GB + u.get("storageGBHour", 0) * P_DISCO


def estado(**kw):
    d = json.load(open(ESTADO, encoding="utf-8")) if os.path.exists(ESTADO) else {}
    d.update(kw)
    json.dump(d, open(ESTADO, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    return d


def registrador(ruta):
    f = open(ruta, "w", encoding="utf-8")

    def log(t=""):
        t = oculto(t)
        print(t, flush=True)
        f.write(t + "\n")
        f.flush()
    return log


# ======================================================================= gold (EMR)
def gold():
    import gzip
    import re
    os.makedirs(LOGS_DIR, exist_ok=True)
    log = registrador(os.path.join(EVID, "aws_05_gold.txt"))
    log("=" * 92)
    log(" AWS paso 4 - CAPA GOLD en EMR Serverless (sedalib-spark, emr-7.9.0 / Spark 3.5.5, us-east-2)")
    log("=" * 92)
    log("Referencia de ubigeo: INEI, UBIGEO 2022 - 1891 distritos (Plataforma Nacional de Datos Abiertos)")
    log("   URL de descarga : %s" % UBIGEO_URL)
    log("   pagina          : %s" % UBIGEO_PAGINA)
    log("   descargado el   : %s | archivo UBIGEOS_2022_1891_distritos.csv (ISO-8859-1, ';') -> raw/referencia/"
        "ubigeo_inei_2022.csv" % UBIGEO_FECHA)
    for local, clave in [(os.path.join(BASE, "src", "comun.py"), "scripts/comun.py"),
                         (os.path.join(BASE, "src", "gold.py"), "scripts/gold.py"),
                         (os.path.join(BASE, "aws", "emr_lanzador.py"), "scripts/emr_lanzador.py"),
                         (UBIGEO_LOCAL, "raw/referencia/ubigeo_inei_2022.csv")]:
        s3.upload_file(local, BUCKET, clave)
        log("subido %s" % clave)
    env = {"SEDALIB_SPARK_MASTER": "auto", "SEDALIB_DATALAKE": S3}
    conf = {"spark.driver.cores": "2", "spark.driver.memory": "7g", "spark.executor.cores": "2",
            "spark.executor.memory": "7g", "spark.dynamicAllocation.maxExecutors": "3"}
    for k, v in env.items():
        conf["spark.emr-serverless.driverEnv." + k] = v
        conf["spark.executorEnv." + k] = v
    params = "--py-files %sscripts/comun.py,%sscripts/gold.py " % (S3, S3) + " ".join("--conf %s=%s" % kv for kv in conf.items())
    jid = emr.start_job_run(
        applicationId=APP_ID, executionRoleArn=ROL, name="sedalib-05-gold",
        jobDriver={"sparkSubmit": {"entryPoint": S3 + "scripts/emr_lanzador.py", "entryPointArguments": ["gold.py"],
                                   "sparkSubmitParameters": params}},
        configurationOverrides={"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": S3 + "logs/"}}},
        tags={"Proyecto": "SEDALIB", "Curso": "BigData-UPAO", "Uso": "gold"})["jobRunId"]
    log("job sedalib-05-gold lanzado: %s" % jid)
    while True:
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=jid)["jobRun"]
        if jr["state"] in ("SUCCESS", "FAILED", "CANCELLED"):
            break
        time.sleep(20)
    for _ in range(12):
        if (jr.get("totalResourceUtilization") or {}).get("vCPUHour"):
            break
        time.sleep(10)
        jr = emr.get_job_run(applicationId=APP_ID, jobRunId=jid)["jobRun"]
    textos = {}
    for tipo in ("stdout", "stderr"):
        clave = "logs/applications/%s/jobs/%s/SPARK_DRIVER/%s.gz" % (APP_ID, jid, tipo)
        for _ in range(12):
            try:
                textos[tipo] = gzip.decompress(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read()).decode("utf-8", "replace")
                break
            except ClientError:
                time.sleep(10)
        with open(os.path.join(LOGS_DIR, "gold_%s.txt" % tipo), "w", encoding="utf-8") as f:
            f.write(oculto(textos.get(tipo, "")))
    u = jr.get("totalResourceUtilization") or {}
    b = jr.get("billedResourceUtilization") or {}
    m = re.search(r"\[lanzador\] tiempo del script \S+: ([\d.]+) s", textos.get("stdout", ""))
    r = dict(id=jid, estado=jr["state"], dur_s=jr.get("totalExecutionDurationSeconds"),
             script_s=float(m.group(1)) if m else None, vcpu_h=u.get("vCPUHour", 0), gb_h=u.get("memoryGBHour", 0),
             disco_gb_h=u.get("storageGBHour", 0), costo=costo(u), costo_facturado=costo(b))
    estado(gold=r)
    log("\n1. JOB (get-job-run; costo real = vCPU-h x %.6f + GB-h x %.7f + disco GB-h x %.6f)" % (P_VCPU, P_GB, P_DISCO))
    log("%-16s %-18s %-9s %7s %9s %8s %8s %9s %9s" % ("job", "id", "estado", "dur(s)", "script(s)", "vCPU-h",
                                                    "GB-h", "real USD", "fact. USD"))
    log("%-16s %-18s %-9s %7s %9s %8.4f %8.4f %9.4f %9.4f" % ("sedalib-05-gold", jid, r["estado"], r["dur_s"],
        "%.1f" % r["script_s"] if r["script_s"] else "-", r["vcpu_h"], r["gb_h"], r["costo"], r["costo_facturado"]))
    if jr["state"] != "SUCCESS":
        log("detalle: %s" % oculto(jr.get("stateDetails", ""))[:500])
        for l in [x for x in textos.get("stderr", "").splitlines() if "Error" in x or "assert" in x.lower()][-10:]:
            log("   " + l[:220])
        sys.exit("El job gold fallo: se detiene (no se reintenta).")
    log("\n2. SALIDA DEL SCRIPT src/gold.py (stdout del driver)")
    for l in textos["stdout"].splitlines():
        if not l.startswith("[lanzador] copiado"):
            log(l)
    log("\n3. ARCHIVOS EN s3://<bucket>/%s" % GOLD_PREFIX)
    tot = 0
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=GOLD_PREFIX):
        for o in p.get("Contents", []):
            tot += o["Size"]
            log("   %-80s %10d B" % (o["Key"], o["Size"]))
    log("   total %.1f KB" % (tot / 1024))


# ======================================================================= Athena / Glue
class Bloqueo(Exception):
    pass


def consulta(sql, db=None):
    kw = dict(QueryString=sql, ResultConfiguration={"OutputLocation": ATHENA_OUT}, WorkGroup="primary")
    if db:
        kw["QueryExecutionContext"] = {"Database": db}
    try:
        qid = athena.start_query_execution(**kw)["QueryExecutionId"]
    except ClientError as e:
        raise Bloqueo("start_query_execution: %s - %s" % (e.response["Error"]["Code"], e.response["Error"]["Message"]))
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    if q["Status"]["State"] != "SUCCEEDED":
        raise Bloqueo("consulta %s %s: %s" % (qid, q["Status"]["State"], q["Status"].get("StateChangeReason", "")))
    filas = []
    if q.get("StatementType") == "DML":
        for p in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            for r in p["ResultSet"]["Rows"]:
                filas.append([c.get("VarCharValue", "") for c in r["Data"]])
    st = q["Statistics"]
    return dict(id=qid, filas=filas, bytes=st.get("DataScannedInBytes", 0),
                ms=st.get("EngineExecutionTimeInMillis", 0), tipo=q.get("StatementType"))


def esquema_parquet(tabla):
    import pyarrow.parquet as pq
    keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=GOLD_PREFIX + tabla + "/")
            for o in p.get("Contents", []) if o["Key"].endswith(".parquet")]
    sch = pq.read_schema(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read()))
    mapa = {"int32": "int", "int64": "bigint", "double": "double", "float": "float", "string": "string",
            "large_string": "string", "bool": "boolean"}
    return [(f.name, mapa[str(f.type)]) for f in sch]


Q1 = """-- Q1. Top 10 distritos de La Libertad por riesgo (holdout 2024-01..05)
SELECT h.distrito, d.provincia, h.ubigeo,
       ROUND(AVG(h.prob_interrupcion), 4) AS prob_media,
       SUM(h.alerta) AS meses_en_alerta,
       SUM(h.interrupcion_real) AS meses_con_interrupcion
FROM gold_riesgo_distrito_mes h
JOIN dim_distrito d ON d.departamento = h.departamento AND d.distrito = h.distrito
WHERE h.departamento = 'LA LIBERTAD' AND h.anio = 2024
GROUP BY h.distrito, d.provincia, h.ubigeo
ORDER BY prob_media DESC
LIMIT 10"""

Q2 = """-- Q2. Alertas por mes frente a interrupciones reales (nacional y La Libertad)
SELECT h.anio, h.mes, t.temporada_lluvia,
       SUM(h.alerta) AS alertas,
       SUM(h.interrupcion_real) AS interrupciones_reales,
       SUM(CASE WHEN h.departamento = 'LA LIBERTAD' THEN h.alerta ELSE 0 END) AS alertas_ll,
       SUM(CASE WHEN h.departamento = 'LA LIBERTAD' THEN h.interrupcion_real ELSE 0 END) AS reales_ll
FROM gold_riesgo_distrito_mes h
JOIN dim_tiempo t ON t.anio = h.anio AND t.mes = h.mes
GROUP BY h.anio, h.mes, t.temporada_lluvia
ORDER BY h.anio, h.mes"""

Q3 = """-- Q3. Recall de las alertas en el holdout 2024 (TP / (TP + FN))
SELECT ambito, SUM(tp) AS tp, SUM(fn) AS fn, SUM(fp) AS fp,
       ROUND(CAST(SUM(tp) AS DOUBLE) / (SUM(tp) + SUM(fn)), 4) AS recall,
       ROUND(CAST(SUM(tp) AS DOUBLE) / (SUM(tp) + SUM(fp)), 4) AS precision
FROM (
  SELECT CASE WHEN departamento = 'LA LIBERTAD' THEN 'LA LIBERTAD' ELSE 'RESTO' END AS ambito,
         CASE WHEN alerta = 1 AND interrupcion_real = 1 THEN 1 ELSE 0 END AS tp,
         CASE WHEN alerta = 0 AND interrupcion_real = 1 THEN 1 ELSE 0 END AS fn,
         CASE WHEN alerta = 1 AND interrupcion_real = 0 THEN 1 ELSE 0 END AS fp
  FROM gold_riesgo_distrito_mes WHERE anio = 2024
  UNION ALL
  SELECT 'NACIONAL',
         CASE WHEN alerta = 1 AND interrupcion_real = 1 THEN 1 ELSE 0 END,
         CASE WHEN alerta = 0 AND interrupcion_real = 1 THEN 1 ELSE 0 END,
         CASE WHEN alerta = 1 AND interrupcion_real = 0 THEN 1 ELSE 0 END
  FROM gold_riesgo_distrito_mes WHERE anio = 2024
) x
WHERE ambito IN ('NACIONAL', 'LA LIBERTAD')
GROUP BY ambito
ORDER BY ambito DESC"""


def tabla_txt(filas):
    if not filas:
        return ["   (sin filas)"]
    an = [max(len(str(f[i])) for f in filas) for i in range(len(filas[0]))]
    out = ["   " + "  ".join(str(c).rjust(an[i]) for i, c in enumerate(filas[0])),
           "   " + "  ".join("-" * a for a in an)]
    out += ["   " + "  ".join(str(c).rjust(an[i]) for i, c in enumerate(f)) for f in filas[1:]]
    return out


def athena_glue():
    log = registrador(os.path.join(EVID, "aws_05_athena.txt"))
    log("=" * 92)
    log(" AWS paso 4 - GLUE DATA CATALOG (DDL desde Athena, sin crawler) y CONSULTAS ATHENA (us-east-2)")
    log("=" * 92)
    log("Workgroup primary (Athena engine v3) | resultados en s3://<bucket>/logs/athena/ | base Glue: %s" % DB)
    log("Costo Athena: %.1f USD por TB escaneado, minimo 10 MB por consulta; DDL sin costo." % P_ATHENA_TB)
    res = {"ddl": [], "q": []}
    try:
        log("\n1. DDL")
        r = consulta("CREATE DATABASE IF NOT EXISTS %s COMMENT 'SEDALIB capa gold (paso 4)'" % DB)
        log("   CREATE DATABASE IF NOT EXISTS %s -> OK (%s)" % (DB, r["id"]))
        res["ddl"].append(r)
        for t in TABLAS:
            cols = esquema_parquet(t)
            consulta("DROP TABLE IF EXISTS %s" % t, DB)
            ddl = ("CREATE EXTERNAL TABLE %s (\n  %s\n)\nSTORED AS PARQUET\nLOCATION '%s%s%s/'"
                   % (t, ",\n  ".join("%s %s" % c for c in cols), S3, GOLD_PREFIX, t))
            r = consulta(ddl, DB)
            res["ddl"].append(r)
            log("\n" + ddl + ";")
            log("   -> OK (%s)" % r["id"])
        log("\n2. TABLAS REGISTRADAS EN GLUE (glue:GetTables)")
        glue = ses.client("glue")
        for t in glue.get_tables(DatabaseName=DB)["TableList"]:
            sd = t["StorageDescriptor"]
            log("   %-26s %-14s %2d columnas  %s" % (t["Name"], t.get("TableType"), len(sd["Columns"]), sd["Location"]))
        log("\n3. FILAS POR TABLA (COUNT(*) en Athena)")
        conteos = []
        for t in TABLAS:
            r = consulta("SELECT COUNT(*) FROM %s" % t, DB)
            conteos.append((t, r["filas"][1][0], r["bytes"]))
            log("   %-26s %7s filas | escaneado %d B" % (t, r["filas"][1][0], r["bytes"]))
        log("\n4. CONSULTAS")
        for sql in (Q1, Q2, Q3):
            r = consulta(sql, DB)
            fact = max(r["bytes"], 10 * 1024 ** 2)
            r.update(sql=sql, costo=fact / 1024 ** 4 * P_ATHENA_TB)
            res["q"].append(r)
            log("\n" + sql + ";")
            log("   id %s | datos escaneados %d B (%.1f KB) | facturable %d MB (minimo) | %.8f USD | motor %d ms | %d filas"
                % (r["id"], r["bytes"], r["bytes"] / 1024, fact // 1024 ** 2, r["costo"], r["ms"], len(r["filas"]) - 1))
            for l in tabla_txt(r["filas"]):
                log(l)
        c_q = sum(q["costo"] for q in res["q"])
        c_cnt = len(conteos) * 10 / 1024 ** 2 * P_ATHENA_TB   # 10 MB minimo cada una (cota superior)
        log("\n5. COSTO ATHENA: 3 consultas = %.8f USD | 5 COUNT(*) de control = %.8f USD | DDL = 0 | total %.8f USD"
            % (c_q, c_cnt, c_q + c_cnt))
        estado(athena=dict(consultas=[{k: q[k] for k in ("id", "bytes", "ms", "costo", "filas")} for q in res["q"]],
                           conteos=conteos, costo=c_q + c_cnt))
    except Bloqueo as e:
        log("\nERROR (se detiene, sin alternativas): %s" % e)
        sys.exit(1)
    except ClientError as e:
        log("\nERROR (se detiene, sin alternativas): %s - %s" % (e.response["Error"]["Code"], e.response["Error"]["Message"]))
        sys.exit(1)


# ======================================================================= exportar al dashboard
def exportar():
    import pyarrow.parquet as pq
    dest = os.path.join(BASE, "dashboard", "data")
    os.makedirs(dest, exist_ok=True)
    for t in TABLAS:
        keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=GOLD_PREFIX + t + "/")
                for o in p.get("Contents", []) if o["Key"].endswith(".parquet")]
        df = pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read())).to_pandas()
        df.to_csv(os.path.join(dest, t + ".csv"), index=False, encoding="utf-8")
        print("dashboard/data/%s.csv  %d filas x %d columnas" % (t, len(df), df.shape[1]))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--gold", action="store_true")
    g.add_argument("--athena", action="store_true")
    g.add_argument("--exportar", action="store_true")
    a = ap.parse_args()
    gold() if a.gold else athena_glue() if a.athena else exportar()
