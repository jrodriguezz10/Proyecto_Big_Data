"""Vistas de terminal para las capturas del paso 4 (sin claves; bucket e ID de cuenta enmascarados).

    python aws/ver_aws05.py glue     -> tablas de sedalib_gold en Glue Data Catalog (glue:GetTables)
    python aws/ver_aws05.py athena   -> ejecuta Q1 (top 10 La Libertad) en Athena y muestra el resultado
"""
import sys
import time

import boto3

REGION = "us-east-2"
BUCKET = "bigdata-lalibertad-jheyson-856152566912"
CUENTA = "856152566912"
DB = "sedalib_gold"
ses = boto3.session.Session(region_name=REGION)


def m(t):
    return str(t).replace(BUCKET, "<bucket>").replace(CUENTA, "<CUENTA>")


def glue():
    g = ses.client("glue")
    d = g.get_database(Name=DB)["Database"]
    print("AWS Glue Data Catalog (%s) - base %s: %s" % (REGION, d["Name"], d.get("Description", "")))
    print("creada %s | tablas registradas con CREATE EXTERNAL TABLE desde Athena (sin crawler)\n"
          % d["CreateTime"].strftime("%Y-%m-%d %H:%M"))
    print("%-26s %-15s %-8s %4s  %s" % ("tabla", "tipo", "formato", "cols", "ubicacion"))
    print("-" * 104)
    for t in sorted(g.get_tables(DatabaseName=DB)["TableList"], key=lambda x: x["Name"]):
        sd = t["StorageDescriptor"]
        fmt = "Parquet" if "parquet" in sd.get("SerdeInfo", {}).get("SerializationLibrary", "").lower() else "?"
        print("%-26s %-15s %-8s %4d  %s" % (t["Name"], t.get("TableType"), fmt, len(sd["Columns"]), m(sd["Location"])))
    h = g.get_table(DatabaseName=DB, Name="gold_riesgo_distrito_mes")["Table"]["StorageDescriptor"]["Columns"]
    print("\nColumnas de la tabla de hechos gold_riesgo_distrito_mes:")
    for c in h:
        print("   %-20s %s" % (c["Name"], c["Type"]))


SQL = """SELECT h.distrito, d.provincia, h.ubigeo,
       ROUND(AVG(h.prob_interrupcion), 4) AS prob_media,
       SUM(h.alerta) AS meses_en_alerta,
       SUM(h.interrupcion_real) AS meses_con_interrupcion
FROM gold_riesgo_distrito_mes h
JOIN dim_distrito d ON d.departamento = h.departamento AND d.distrito = h.distrito
WHERE h.departamento = 'LA LIBERTAD' AND h.anio = 2024
GROUP BY h.distrito, d.provincia, h.ubigeo
ORDER BY prob_media DESC
LIMIT 10"""


def athena():
    a = ses.client("athena")
    print("Amazon Athena (%s) | workgroup primary | base %s\n" % (REGION, DB))
    print("-- Q1. Top 10 distritos de La Libertad por riesgo (holdout 2024)")
    print(SQL + ";\n")
    qid = a.start_query_execution(QueryString=SQL, QueryExecutionContext={"Database": DB}, WorkGroup="primary",
                                  ResultConfiguration={"OutputLocation": "s3://%s/logs/athena/" % BUCKET})["QueryExecutionId"]
    while True:
        q = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    st = q["Statistics"]
    print("Estado: %s | id %s | datos escaneados: %.1f KB | tiempo de motor: %d ms\n"
          % (q["Status"]["State"], qid, st.get("DataScannedInBytes", 0) / 1024, st.get("EngineExecutionTimeInMillis", 0)))
    filas = [[c.get("VarCharValue", "") for c in r["Data"]] for r in a.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]]
    an = [max(len(f[i]) for f in filas) for i in range(len(filas[0]))]
    for k, f in enumerate(filas):
        print("  ".join(c.ljust(an[i]) if i < 2 else c.rjust(an[i]) for i, c in enumerate(f)))
        if k == 0:
            print("  ".join("-" * x for x in an))


if __name__ == "__main__":
    {"glue": glue, "athena": athena}[sys.argv[1]]()
