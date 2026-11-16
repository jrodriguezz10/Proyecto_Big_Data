"""Paso 8: costos del proyecto en AWS (us-east-2).

    python aws/08_costos.py --estimado   costo mensual estimado con la AWS Pricing API -> evidencias/aws_08_estimado_mensual.txt
    python aws/08_costos.py --real       costo real de noviembre 2026 por servicio con Cost Explorer (ce:GetCostAndUsage),
                                         sin creditos (RECORD_TYPE=Usage) y con creditos -> evidencias/aws_08_costo_real.txt

Precios de lista on-demand de US East (Ohio). Capas gratuitas indicadas aparte (no se restan del estimado).
Sin ID de cuenta en la salida.
"""
import argparse
import datetime as dt
import json
import os
import sys

import boto3

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UBICACION = "US East (Ohio)"
HORAS_MES = 730
NOTA_CALCULADORA = """Calculadora AWS (calculator.aws, enlace publico, armada el 2026-11-16 con perfil temporal):
  https://calculator.aws/#/estimate?id=7ba8b66fcc0542dd34b1c90b99de62824e42ab3c
  Total calculadora: 6.57 USD/mes (78.84 USD/12 meses). Por servicio: S3 0.03 | EMR 0.76 | Glue 0.00 | Athena 0.05 |
  EC2+EBS 1.98 | VPC IPv4 3.65 | CloudFront 0.10.
  Diferencia con la API (7.05): -0.48 USD. Casi toda es el EBS: la calculadora prorratea el gp3 de 8 GB a las 176 h de
  la instancia (8 x 0.08 x 176/730 = 0.15 USD) y la API cobra el mes completo (0.64 USD). En este proyecto el disco
  existe las 730 h aunque EC2 este detenida, asi que el costo real se acerca a la cifra de la API. El resto: Glue
  0.001 USD que la calculadora deja en 0 por la capa gratuita, y redondeos."""
pr = boto3.client("pricing", region_name="us-east-1")


def precio(servicio, usagetype, desde="0", extra=None):
    f = {"location": UBICACION, "usagetype": usagetype}
    if servicio == "AmazonCloudFront":
        f = {"usagetype": usagetype}
    f.update(extra or {})
    lst = pr.get_products(ServiceCode=servicio, MaxResults=50,
                          Filters=[{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in f.items()])["PriceList"]
    for raw in lst:
        p = json.loads(raw)
        for t in p["terms"]["OnDemand"].values():
            for d in t["priceDimensions"].values():
                if d.get("beginRange", "0") == desde:
                    return float(d["pricePerUnit"]["USD"]), d["description"]
    sys.exit("Sin precio para %s %s" % (servicio, usagetype))


def precio_t3micro():
    lst = pr.get_products(ServiceCode="AmazonEC2", MaxResults=10, Filters=[
        {"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in
        {"instanceType": "t3.micro", "location": UBICACION, "operatingSystem": "Linux", "tenancy": "Shared",
         "preInstalledSw": "NA", "capacitystatus": "Used", "licenseModel": "No License required"}.items()])["PriceList"]
    p = json.loads(lst[0])
    d = next(iter(next(iter(p["terms"]["OnDemand"].values()))["priceDimensions"].values()))
    return float(d["pricePerUnit"]["USD"]), d["description"]


def precio_gp3():
    lst = pr.get_products(ServiceCode="AmazonEC2", MaxResults=10, Filters=[
        {"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in
        {"location": UBICACION, "productFamily": "Storage", "volumeApiName": "gp3"}.items()])["PriceList"]
    p = json.loads(lst[0])
    d = next(iter(next(iter(p["terms"]["OnDemand"].values()))["priceDimensions"].values()))
    return float(d["pricePerUnit"]["USD"]), d["description"]


def estimado():
    P = {
        "s3_gb": precio("AmazonS3", "USE2-TimedStorage-ByteHrs"),
        "s3_put": precio("AmazonS3", "USE2-Requests-Tier1"),
        "s3_get": precio("AmazonS3", "USE2-Requests-Tier2"),
        "emr_vcpu": precio("ElasticMapReduce", "USE2-EMR-SERVERLESS-vCPUHours"),
        "emr_gb": precio("ElasticMapReduce", "USE2-EMR-SERVERLESS-MemoryGBHours"),
        "glue_obj": precio("AWSGlue", "USE2-Catalog-Storage"),
        "glue_req": precio("AWSGlue", "USE2-Catalog-Request"),
        "athena_tb": precio("AmazonAthena", "USE2-DataScannedInTB"),
        "t3": precio_t3micro(),
        "gp3": precio_gp3(),
        "ipv4": precio("AmazonVPC", "USE2-PublicIPv4:InUseAddress"),
        "cf_gb": precio("AmazonCloudFront", "US-DataTransfer-Out-Bytes"),
        "cf_req": precio("AmazonCloudFront", "US-Requests-Tier2-HTTPS"),
    }
    emr_h = 5 * 4 * 0.25                                     # 5 ejecuciones x 4 workers x 15 min = 5 worker-h
    athena_tb = 1000 * 10 / 1024 / 1024                      # 0.1 MB se factura como el minimo de 10 MB por consulta

    def filas(horas_ec2):
        return [
            ("S3 Standard", "1 GB almacenado", 1 * P["s3_gb"][0]),
            ("S3 Standard", "1,000 PUT", 1000 * P["s3_put"][0]),
            ("S3 Standard", "10,000 GET", 10000 * P["s3_get"][0]),
            ("EMR Serverless", "5 x 4 workers x 15 min: %.0f vCPU-h" % (emr_h * 2), emr_h * 2 * P["emr_vcpu"][0]),
            ("EMR Serverless", "5 x 4 workers x 15 min: %.0f GB-h" % (emr_h * 8), emr_h * 8 * P["emr_gb"][0]),
            ("Glue Data Catalog", "10 objetos", 10 * P["glue_obj"][0]),
            ("Glue Data Catalog", "1,000 solicitudes", 1000 * P["glue_req"][0]),
            ("Athena", "1,000 consultas x 0.1 MB (min. 10 MB c/u)", athena_tb * P["athena_tb"][0]),
            ("EC2 t3.micro", "%d h" % horas_ec2, horas_ec2 * P["t3"][0]),
            ("EBS gp3", "8 GB-mes", 8 * P["gp3"][0]),
            ("IPv4 publica / IP elastica", "730 h", HORAS_MES * P["ipv4"][0]),
            ("CloudFront", "1 GB salida", 1 * P["cf_gb"][0]),
            ("CloudFront", "10,000 solicitudes HTTPS", 10000 * P["cf_req"][0]),
        ]

    esc = {"Sustentacion / uso academico (EC2 176 h)": filas(176), "Produccion 24/7 (EC2 730 h)": filas(730)}
    L = ["COSTO MENSUAL ESTIMADO - AWS Pricing API, %s, precios de lista on-demand (consultado %s)"
         % (UBICACION, dt.datetime.now().strftime("%Y-%m-%d %H:%M")), ""]
    tot = {}
    nombres = list(esc)
    L.append("%-27s %-44s %12s %12s" % ("servicio", "uso mensual", "academico", "24/7"))
    L.append("-" * 98)
    for (s, u, v1), (_, u2, v2) in zip(esc[nombres[0]], esc[nombres[1]]):
        uso = u if u == u2 else u.replace("176 h", "176 h | 730 h")
        L.append("%-27s %-44s %12.4f %12.4f" % (s, uso, v1, v2))
    for n in nombres:
        tot[n] = sum(v for _, _, v in esc[n])
    L.append("-" * 98)
    L.append("%-72s %12.2f %12.2f   USD/mes" % ("TOTAL", tot[nombres[0]], tot[nombres[1]]))
    L += ["",
          "Precios unitarios usados:",
          "  S3 %.3f USD/GB-mes | PUT %.3f USD/1k | GET %.4f USD/1k" % (P["s3_gb"][0], P["s3_put"][0] * 1000,
                                                                       P["s3_get"][0] * 1000),
          "  EMR Serverless %.6f USD/vCPU-h | %.7f USD/GB-h (x86; 20 GB de disco por worker incluidos)"
          % (P["emr_vcpu"][0], P["emr_gb"][0]),
          "  Glue Catalog %.0f USD/100k objetos | %.0f USD/millon de solicitudes" % (P["glue_obj"][0] * 1e5,
                                                                                    P["glue_req"][0] * 1e6),
          "  Athena %.2f USD/TB escaneado (minimo 10 MB por consulta)" % P["athena_tb"][0],
          "  EC2 t3.micro %.4f USD/h | EBS gp3 %.3f USD/GB-mes | IPv4 publica %.3f USD/h"
          % (P["t3"][0], P["gp3"][0], P["ipv4"][0]),
          "  CloudFront %.3f USD/GB (EE.UU.) | %.4f USD/10k HTTPS" % (P["cf_gb"][0], P["cf_req"][0] * 1e4),
          "",
          "Capas gratuitas (no restadas arriba): Glue Data Catalog 1 M objetos y 1 M solicitudes/mes; CloudFront 1 TB",
          "y 10 M solicitudes/mes (siempre gratis). Restandolas, Glue y CloudFront quedan en 0.",
          "La IP elastica se cobra 730 h aunque EC2 este detenida (por eso el escenario academico la incluye completa).",
          ""] + NOTA_CALCULADORA.splitlines()
    out = os.path.join(BASE, "evidencias", "aws_08_estimado_mensual.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))


def real():
    ce = boto3.client("ce", region_name="us-east-1")
    hoy = dt.date.today()
    ini, fin = "2026-11-01", min(hoy + dt.timedelta(days=1), dt.date(2026, 12, 1)).isoformat()

    def consulta(filtro):
        kw = dict(TimePeriod={"Start": ini, "End": fin}, Granularity="MONTHLY", Metrics=["UnblendedCost"],
                  GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        if filtro:
            kw["Filter"] = filtro
        r = ce.get_cost_and_usage(**kw)
        out = {}
        for per in r["ResultsByTime"]:
            for g in per["Groups"]:
                out[g["Keys"][0]] = out.get(g["Keys"][0], 0) + float(g["Metrics"]["UnblendedCost"]["Amount"])
        return out

    uso = consulta({"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}})
    neto = consulta(None)
    servicios = sorted(set(uso) | set(neto), key=lambda s: -uso.get(s, 0))
    L = ["COSTO REAL - Cost Explorer (ce:GetCostAndUsage), %s a %s (fin exclusivo), por servicio, UnblendedCost USD"
         % (ini, fin), "",
         "%-48s %16s %16s" % ("servicio", "sin creditos", "con creditos"),
         "%-48s %16s %16s" % ("", "(RECORD_TYPE=Usage)", "(todos los tipos)"), "-" * 82]
    for s in servicios:
        if abs(uso.get(s, 0)) < 0.000005 and abs(neto.get(s, 0)) < 0.000005:
            continue
        L.append("%-48s %16.4f %16.4f" % (s[:48], uso.get(s, 0), neto.get(s, 0)))
    L += ["-" * 82, "%-48s %16.4f %16.4f" % ("TOTAL", sum(uso.values()), sum(neto.values())), "",
          "Cost Explorer se actualiza con hasta 24 h de retraso; los ultimos dias pueden estar incompletos."]
    out = os.path.join(BASE, "evidencias", "aws_08_costo_real.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--estimado", action="store_true")
    g.add_argument("--real", action="store_true")
    a = ap.parse_args()
    estimado() if a.estimado else real()
