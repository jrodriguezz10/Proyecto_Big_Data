"""Vistas de terminal para las capturas del paso 4 (sin claves; bucket e ID de cuenta enmascarados).

    python aws/ver_aws05.py glue     -> tablas de sedalib_gold en Glue Data Catalog (glue:GetTables)
    python aws/ver_aws05.py athena   -> ejecuta Q1 (top 10 La Libertad) en Athena y muestra el resultado
    python aws/ver_aws05.py q2q3     -> ejecuta Q2 (alertas vs reales por mes) y Q3 (recall 2024) con datos escaneados
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


def _sql(nombre):
    """Lee Q2/Q3 de aws/05_gold_athena.py sin ejecutarlo (mismas consultas de la evidencia)."""
    import ast
    import os
    arbol = ast.parse(open(os.path.join(os.path.dirname(__file__), "05_gold_athena.py"), encoding="utf-8").read())
    for n in arbol.body:
        if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == nombre:
            return n.value.value
    raise KeyError(nombre)


def _correr(a, sql):
    cuerpo = "\n".join(l for l in sql.splitlines() if not l.startswith("--"))
    qid = a.start_query_execution(QueryString=cuerpo, QueryExecutionContext={"Database": DB}, WorkGroup="primary",
                                  ResultConfiguration={"OutputLocation": "s3://%s/logs/athena/" % BUCKET})["QueryExecutionId"]
    while True:
        q = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if q["Status"]["State"] != "SUCCEEDED":
        sys.exit(m(q["Status"].get("StateChangeReason", q["Status"]["State"])))
    filas = [[c.get("VarCharValue", "") for c in r["Data"]]
             for r in a.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]]
    return q, qid, filas


def q2q3():
    a = ses.client("athena")
    print("Amazon Athena (%s) | workgroup primary | base %s | tablas en s3://<bucket>/curated/gold/" % (REGION, DB))
    q, qid, f = _correr(a, _sql("Q2"))
    st = q["Statistics"]
    print("\n-- Q2. Alertas del modelo frente a interrupciones reales por mes (nacional y La Libertad)")
    print("   id %s... | %s | escaneado %.1f KB | motor %d ms | %d meses"
          % (qid[:8], q["Status"]["State"], st["DataScannedInBytes"] / 1024, st["EngineExecutionTimeInMillis"], len(f) - 1))
    cab = "%-7s %3s %7s %6s %7s %6s" % ("mes", "llu", "alertas", "reales", "alert_LL", "real_LL")
    datos = ["%4s-%02d %3s %7s %6s %7s %6s" % (r[0], int(r[1]), r[2], r[3], r[4], r[5], r[6]) for r in f[1:]]
    mitad = (len(datos) + 1) // 2
    print("   " + cab + "      " + cab)
    for i in range(mitad):
        der = datos[i + mitad] if i + mitad < len(datos) else ""
        print("   " + datos[i] + "      " + der)
    q, qid, f = _correr(a, _sql("Q3"))
    st = q["Statistics"]
    print("\n-- Q3. Recall y precision de las alertas en el holdout 2024 (TP / (TP + FN))")
    print("   id %s... | %s | escaneado %.1f KB | motor %d ms" % (qid[:8], q["Status"]["State"],
                                                                st["DataScannedInBytes"] / 1024,
                                                                st["EngineExecutionTimeInMillis"]))
    for k, r in enumerate(f):
        print("   " + "  ".join(c.ljust(12) if i == 0 else c.rjust(8) for i, c in enumerate(r)))
        if k == 0:
            print("   " + "-" * 60)
    print("\nllu = temporada de lluvia (dic-abr). 2024-05 incompleto en el origen (SUNASS termina el 2024-05-07).")
    print("Athena factura 5 USD/TB con minimo 10 MB por consulta: 2 consultas = ~0.0001 USD.")


if __name__ == "__main__":
    {"glue": glue, "athena": athena, "q2q3": q2q3}[sys.argv[1]]()
