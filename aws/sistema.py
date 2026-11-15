"""Encender / apagar / destruir todo el sistema SEDALIB en AWS (us-east-2). Sin claves en la instancia.

    python aws/sistema.py --estado                  EC2, EMR, CloudFront, IP elastica + costo por hora actual
    python aws/sistema.py --encender                inicia EC2 y espera a que https://<dominio>.cloudfront.net responda
    python aws/sistema.py --encender --actualizar   ademas, ANTES de encender: pipeline en EMR (03), gold.py (05 --gold),
                                                    exporta dashboard/data (05 --exportar + src/gold_impacto.py) y MUESTRA
                                                    que cambio. NO hace commit ni push: el usuario los revisa y publica
                                                    (la instancia hace git pull en cada arranque)
    python aws/sistema.py --apagar                  detiene EC2; si EMR no tiene jobs activos, detiene la app EMR
    python aws/sistema.py --destruir                pide escribir DESTRUIR; borra CloudFront, libera la IP elastica, termina
                                                    EC2, borra el security group y la app EMR. NO toca S3.

Recursos (se encuentran por etiqueta Proyecto=SEDALIB, nunca por ID guardado):
  EC2 sedalib-dashboard + SG sedalib-dashboard-sg (paso 6), IP elastica sedalib-dashboard-eip y distribucion
  CloudFront "SEDALIB dashboard" (paso 7, aws/07_cloudfront.py), app EMR Serverless sedalib-spark (paso 2).
Salidas sin ID de cuenta; IDs de recursos enmascarados.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.request

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
UBICACION = "US East (Ohio)"
NOMBRE = "sedalib-dashboard"
SG = "sedalib-dashboard-sg"
EIP = "sedalib-dashboard-eip"
CF_COMENTARIO = "SEDALIB dashboard"
APP_EMR = "sedalib-spark"
PUERTO = 8501
DISCO_GB = 8
HORAS_MES = 730
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TZ = dt.timezone(dt.timedelta(hours=-5))

ses = boto3.session.Session(region_name=REGION)
ec2 = ses.client("ec2")
emr = ses.client("emr-serverless")
cf = boto3.client("cloudfront")


# ---------------------------------------------------------------- busqueda por etiqueta
def mask(i):
    return "-" if not i else (i[:2] + "..." + i[-4:] if len(i) > 8 else i[:1] + "..." + i[-3:])


def instancia(estados=("pending", "running", "stopping", "stopped", "shutting-down")):
    r = ec2.describe_instances(Filters=[{"Name": "tag:Proyecto", "Values": ["SEDALIB"]},
                                        {"Name": "tag:Name", "Values": [NOMBRE]},
                                        {"Name": "instance-state-name", "Values": list(estados)}])
    l = [i for res in r["Reservations"] for i in res["Instances"]]
    return l[0] if l else None


def grupo():
    r = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG]},
                                              {"Name": "tag:Proyecto", "Values": ["SEDALIB"]}])["SecurityGroups"]
    return r[0] if r else None


def ip_elastica():
    r = ec2.describe_addresses(Filters=[{"Name": "tag:Proyecto", "Values": ["SEDALIB"]},
                                        {"Name": "tag:Name", "Values": [EIP]}])["Addresses"]
    return r[0] if r else None


def dns_publico(ip):
    return "ec2-%s.%s.compute.amazonaws.com" % (ip.replace(".", "-"), REGION)


def distribucion():
    """Distribucion con comentario CF_COMENTARIO y etiqueta Proyecto=SEDALIB (o None)."""
    pag = cf.get_paginator("list_distributions")
    for p in pag.paginate():
        for d in p.get("DistributionList", {}).get("Items", []):
            if d.get("Comment") == CF_COMENTARIO:
                tags = cf.list_tags_for_resource(Resource=d["ARN"])["Tags"].get("Items", [])
                if {"Key": "Proyecto", "Value": "SEDALIB"} in tags:
                    return d
    return None


def app_emr():
    for a in emr.list_applications()["applications"]:
        if a["name"] == APP_EMR and a["state"] != "TERMINATED":
            return a
    return None


def jobs_activos(app_id):
    act = ["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "QUEUED", "CANCELLING"]
    return emr.list_job_runs(applicationId=app_id, states=act).get("jobRuns", [])


def responde(url, timeout=10):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "sedalib-check"}),
                                    timeout=timeout) as r:
            return r.status
    except Exception:
        return None


# ---------------------------------------------------------------- precios (Pricing API)
def _precio(servicio, filtros):
    pr = boto3.client("pricing", region_name="us-east-1")
    f = [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filtros.items()]
    out = []
    for raw in pr.get_products(ServiceCode=servicio, Filters=f, MaxResults=50)["PriceList"]:
        p = json.loads(raw)
        for t in p.get("terms", {}).get("OnDemand", {}).values():
            for d in t["priceDimensions"].values():
                out.append((float(d["pricePerUnit"].get("USD", 0)), d.get("beginRange", "0"), d["description"]))
    return out


def precios():
    inst = _precio("AmazonEC2", {"instanceType": "t3.micro", "location": UBICACION, "operatingSystem": "Linux",
                                 "tenancy": "Shared", "preInstalledSw": "NA", "capacitystatus": "Used",
                                 "licenseModel": "No License required"})
    uso = _precio("AmazonVPC", {"location": UBICACION, "usagetype": "USE2-PublicIPv4:InUseAddress"})
    ocio = _precio("AmazonVPC", {"location": UBICACION, "usagetype": "USE2-PublicIPv4:IdleAddress"})
    gp3 = _precio("AmazonEC2", {"location": UBICACION, "productFamily": "Storage", "volumeApiName": "gp3"})
    cf_gb = [x for x in _precio("AmazonCloudFront", {"usagetype": "US-DataTransfer-Out-Bytes"}) if x[1] == "0"]
    cf_req = _precio("AmazonCloudFront", {"usagetype": "US-Requests-Tier2-HTTPS"})
    return {"t3micro": inst[0][0], "ipv4_uso": uso[0][0], "ipv4_ocio": ocio[0][0],
            "disco_h": gp3[0][0] * DISCO_GB / HORAS_MES, "gp3_gb_mes": gp3[0][0],
            "cf_gb": cf_gb[0][0] if cf_gb else None, "cf_10k": cf_req[0][0] * 10000 if cf_req else None}


def costo_hora(p, ec2_estado, hay_eip, hay_ec2=True):
    """(USD/h, detalle) segun el estado actual. EMR y CloudFront: 0 fijo (pago por uso)."""
    det, h = [], 0.0
    if hay_ec2 and ec2_estado in ("running", "pending"):
        det.append(("instancia t3.micro", p["t3micro"])); h += p["t3micro"]
        det.append(("IPv4 publica en uso" + (" (IP elastica)" if hay_eip else ""), p["ipv4_uso"])); h += p["ipv4_uso"]
    elif hay_eip:
        det.append(("IP elastica sin uso (EC2 detenida)", p["ipv4_ocio"])); h += p["ipv4_ocio"]
    if hay_ec2:
        det.append(("disco gp3 %d GB" % DISCO_GB, p["disco_h"])); h += p["disco_h"]
    det.append(("EMR Serverless (sin jobs: 0; cobra por job)", 0.0))
    det.append(("CloudFront (sin cargo fijo; %s USD/GB, %s USD/10k HTTPS)"
                % (p["cf_gb"], "%.4f" % p["cf_10k"] if p["cf_10k"] else "?"), 0.0))
    return h, det


# ---------------------------------------------------------------- modos
def estado():
    print("SISTEMA SEDALIB en AWS (%s) - %s\n" % (REGION, dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M (UTC-05:00)")))
    i = instancia()
    e = ip_elastica()
    d = distribucion()
    a = app_emr()
    print("%-14s %s" % ("EC2", "no existe" if not i else "%s %s | %s | %s" % (
        NOMBRE, mask(i["InstanceId"]), i["InstanceType"], i["State"]["Name"].upper())))
    if e:
        asoc = "asociada a %s" % mask(e.get("InstanceId")) if e.get("InstanceId") else "SIN asociar"
        print("%-14s %s (%s) | %s" % ("IP elastica", e["PublicIp"], dns_publico(e["PublicIp"]), asoc))
    else:
        print("%-14s no existe" % "IP elastica")
    g = grupo()
    if g:
        reglas = []
        for p in g["IpPermissions"]:
            fuentes = [x["CidrIp"] for x in p.get("IpRanges", [])] + [x["PrefixListId"][:5] + "..." for x in p.get("PrefixListIds", [])]
            reglas.append("tcp %s <- %s" % (p.get("FromPort"), ", ".join(fuentes)))
        print("%-14s %s: %s" % ("Security group", SG, "; ".join(reglas) or "sin reglas de entrada"))
    if d:
        url = "https://%s/" % d["DomainName"]
        cod = responde(url) if i and i["State"]["Name"] == "running" else None
        print("%-14s %s | %s | %s | %s" % ("CloudFront", mask(d["Id"]), d["Status"], "habilitada" if d["Enabled"] else "DESHABILITADA",
                                            url + ("  -> HTTP %s" % cod if cod else ("  -> no responde" if i and i["State"]["Name"] == "running" else ""))))
    else:
        print("%-14s no existe" % "CloudFront")
    if a:
        n = len(jobs_activos(a["id"]))
        print("%-14s %s | %s | jobs activos: %d" % ("EMR", APP_EMR, a["state"], n))
    else:
        print("%-14s no existe" % "EMR")
    print("%-14s bucket del datalake: se conserva siempre (no lo toca este script)" % "S3")
    try:
        p = precios()
        h, det = costo_hora(p, i["State"]["Name"] if i else None, bool(e), bool(i))
        print("\nCosto por hora AHORA (Pricing API, %s):" % UBICACION)
        for k, v in det:
            print("  %-58s %8.4f USD/h" % (k, v))
        print("  %-58s %8.4f USD/h | %.4f USD/dia" % ("TOTAL", h, 24 * h))
    except ClientError as ex:
        print("\nCosto: no disponible (%s)" % ex.response["Error"]["Code"])


def _mostrar_cambios_datos():
    """Muestra que cambio en dashboard/data. NO hace commit ni push: lo decide el usuario."""
    def git(*a):
        return subprocess.run(["git", *a], cwd=BASE, capture_output=True, text=True).stdout
    estado_git = git("status", "--porcelain", "dashboard/data").strip()
    print("\n" + "=" * 100 + "\nCAMBIOS EN dashboard/data (sin commit ni push)\n" + "=" * 100)
    if not estado_git:
        print("Sin cambios: los CSV son identicos a los versionados.")
        return False
    print(git("diff", "--stat", "dashboard/data").rstrip() or estado_git)
    for linea in estado_git.splitlines():
        ruta = linea[3:].strip()
        nuevo = sum(1 for _ in open(os.path.join(BASE, ruta), encoding="utf-8")) - 1
        previo = git("show", "HEAD:" + ruta)
        n_prev = max(len(previo.splitlines()) - 1, 0) if previo else 0
        print("  %-48s filas %6d -> %6d (%+d)" % (ruta, n_prev, nuevo, nuevo - n_prev))
    print("\nPara publicarlos (la instancia hace git pull en cada arranque):")
    print("  git add dashboard/data && git commit -m \"Datos del dashboard actualizados\" && git push")
    print("  python aws/sistema.py --apagar && python aws/sistema.py --encender")
    return True


def actualizar():
    py = sys.executable
    pasos = [[py, "aws/03_pipeline_emr.py", "--aplicar"], [py, "aws/05_gold_athena.py", "--gold"],
             [py, "aws/05_gold_athena.py", "--exportar"]]
    for c in pasos:
        print("\n>>> %s" % " ".join(c[1:]))
        if subprocess.run(c, cwd=BASE).returncode != 0:
            sys.exit("Fallo: %s. Detenido (no se reintenta, no se enciende EC2)." % " ".join(c[1:]))
    tmp = os.path.join(BASE, "data", "processed", "master_panel_clima.parquet")
    bucket = boto3.client("sts").get_caller_identity()["Account"]
    bucket = "bigdata-lalibertad-jheyson-" + bucket
    print("\n>>> descarga processed/master_panel_clima.parquet + src/gold_impacto.py")
    subprocess.run(["aws", "s3", "sync", "--only-show-errors", "s3://%s/processed/master_panel_clima.parquet/" % bucket,
                    tmp], cwd=BASE, check=True)
    if subprocess.run([py, "src/gold_impacto.py", "--panel", tmp], cwd=BASE).returncode != 0:
        sys.exit("Fallo src/gold_impacto.py. Detenido.")
    return _mostrar_cambios_datos()


def encender(con_actualizar):
    hay_cambios = actualizar() if con_actualizar else False
    i = instancia()
    if not i:
        sys.exit("No existe la instancia %s." % NOMBRE)
    d = distribucion()
    if not d:
        sys.exit("No existe la distribucion CloudFront (correr aws/07_cloudfront.py --aplicar).")
    t0 = time.time()
    if i["State"]["Name"] == "stopped":
        ec2.start_instances(InstanceIds=[i["InstanceId"]])
        print("iniciando EC2 %s ..." % mask(i["InstanceId"]))
    elif i["State"]["Name"] not in ("running", "pending"):
        sys.exit("EC2 esta en estado %s: esperar y repetir." % i["State"]["Name"])
    ec2.get_waiter("instance_running").wait(InstanceIds=[i["InstanceId"]])
    url = "https://%s/" % d["DomainName"]
    print("EC2 running | esperando %s ..." % url)
    while time.time() - t0 < 15 * 60:
        cod = responde(url + "_stcore/health")
        if cod == 200:
            print("LISTO en %.0f s: %s  (HTTP 200)" % (time.time() - t0, url))
            if hay_cambios:
                print("AVISO: el tablero muestra los datos YA publicados; los nuevos de dashboard/data se ven despues"
                      " del commit + push y un reinicio (ver arriba).")
            estado()
            return url
        time.sleep(10)
    sys.exit("EC2 encendida pero %s no respondio en 15 min (ver --estado)." % url)


def apagar():
    i = instancia(("pending", "running", "stopping", "stopped"))
    if i and i["State"]["Name"] in ("pending", "running"):
        ec2.stop_instances(InstanceIds=[i["InstanceId"]])
        print("deteniendo EC2 %s ..." % mask(i["InstanceId"]))
        ec2.get_waiter("instance_stopped").wait(InstanceIds=[i["InstanceId"]])
    print("EC2: %s" % ("stopped" if i else "no existe"))
    a = app_emr()
    if a:
        act = jobs_activos(a["id"])
        if act:
            print("EMR: %d job(s) activo(s): NO se detiene la app (esperar y repetir --apagar)." % len(act))
        elif a["state"] in ("STARTED", "STARTING", "CREATED"):
            if a["state"] != "CREATED":
                emr.stop_application(applicationId=a["id"])
                while emr.get_application(applicationId=a["id"])["application"]["state"] != "STOPPED":
                    time.sleep(5)
            print("EMR: %s STOPPED (sin jobs activos)" % APP_EMR)
        else:
            print("EMR: %s %s (sin jobs activos)" % (APP_EMR, a["state"]))
    estado()


def destruir():
    print("Se BORRARAN: distribucion CloudFront, IP elastica, instancia EC2 (y su disco), security group %s y la app "
          "EMR %s.\nNO se toca S3 (datalake, evidencias, logs)." % (SG, APP_EMR))
    if input('Escribe DESTRUIR para continuar: ').strip() != "DESTRUIR":
        sys.exit("Cancelado: no se borro nada.")
    d = distribucion()
    if d:
        r = cf.get_distribution_config(Id=d["Id"])
        conf, etag = r["DistributionConfig"], r["ETag"]
        if conf["Enabled"]:
            conf["Enabled"] = False
            etag = cf.update_distribution(Id=d["Id"], IfMatch=etag, DistributionConfig=conf)["ETag"]
            print("CloudFront %s deshabilitada; esperando despliegue (5-15 min)..." % mask(d["Id"]))
        cf.get_waiter("distribution_deployed").wait(Id=d["Id"], WaiterConfig={"Delay": 30, "MaxAttempts": 60})
        etag = cf.get_distribution(Id=d["Id"])["ETag"]
        cf.delete_distribution(Id=d["Id"], IfMatch=etag)
        print("CloudFront borrada")
    e = ip_elastica()
    if e:
        if e.get("AssociationId"):
            ec2.disassociate_address(AssociationId=e["AssociationId"])
        ec2.release_address(AllocationId=e["AllocationId"])
        print("IP elastica %s liberada" % e["PublicIp"])
    i = instancia()
    if i:
        ec2.terminate_instances(InstanceIds=[i["InstanceId"]])
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[i["InstanceId"]])
        print("EC2 %s terminada (disco borrado)" % mask(i["InstanceId"]))
    g = grupo()
    if g:
        for k in range(10):
            try:
                ec2.delete_security_group(GroupId=g["GroupId"])
                print("security group %s borrado" % SG)
                break
            except ClientError as ex:
                if "DependencyViolation" not in str(ex) or k == 9:
                    raise
                time.sleep(15)
    a = app_emr()
    if a:
        if jobs_activos(a["id"]):
            sys.exit("EMR tiene jobs activos: no se borra la app (repetir --destruir cuando terminen).")
        if a["state"] in ("STARTED", "STARTING"):
            emr.stop_application(applicationId=a["id"])
            while emr.get_application(applicationId=a["id"])["application"]["state"] != "STOPPED":
                time.sleep(5)
        emr.delete_application(applicationId=a["id"])
        print("app EMR %s borrada" % APP_EMR)
    print("Listo. S3 se conserva.")
    estado()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    for m in ("estado", "encender", "apagar", "destruir"):
        g.add_argument("--" + m, action="store_true")
    ap.add_argument("--actualizar", action="store_true", help="solo con --encender")
    a = ap.parse_args()
    if a.actualizar and not a.encender:
        ap.error("--actualizar solo se usa con --encender")
    if a.estado:
        estado()
    elif a.encender:
        encender(a.actualizar)
    elif a.apagar:
        apagar()
    else:
        destruir()
