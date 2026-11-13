"""AWS paso 1: data lake S3 configurado por codigo (boto3).

Uso (desde la raiz del proyecto, con un perfil AWS ya configurado; este script NO lee ni imprime claves):
    python aws/01_setup_s3.py            # --plan (por defecto): solo lectura, muestra que cambiaria
    python aws/01_setup_s3.py --aplicar  # aplica los cambios y escribe evidencias/aws_01_s3.txt

a. Identidad (sts get-caller-identity): solo cuenta y ARN.
b. Bucket: bloqueo de acceso publico, cifrado por defecto SSE-S3, versionado, etiquetas
   Proyecto=SEDALIB y Curso=BigData-UPAO. Idempotente: solo cambia lo que difiere.
c. Sube data/raw/ a s3://<bucket>/raw/ conservando subcarpetas; omite archivos que ya existen
   con el mismo tamano. Excepcion: data/raw/clima_nasa_la_libertad.csv -> raw/clima/ (REMAPEO).
d. Lista lo subido y lo compara con los archivos locales.
e. Comprobacion de solo lectura: emr-serverless list-applications, glue get-databases,
   athena list-work-groups.
"""
import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError

BUCKET = "bigdata-lalibertad-jheyson-856152566912"
REGION = "us-east-2"
PREFIJO = "raw/"
TAGS = {"Proyecto": "SEDALIB", "Curso": "BigData-UPAO"}
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_LOCAL = os.path.join(BASE, "data", "raw")
EVIDENCIA = os.path.join(BASE, "evidencias", "aws_01_s3.txt")
BLOQUEO = {"BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True,
           "RestrictPublicBuckets": True}
MB = 1024 * 1024
# Rutas remotas distintas de la local (el archivo local NO se mueve): src/integracion_clima.py lee
# raw/clima/clima_nasa_la_libertad.csv.
REMAPEO = {"clima_nasa_la_libertad.csv": "clima/clima_nasa_la_libertad.csv"}


class Tee:
    """Escribe en consola y en el archivo de evidencia."""

    def __init__(self, ruta):
        self.f = open(ruta, "w", encoding="utf-8") if ruta else None

    def __call__(self, *a):
        linea = " ".join(str(x) for x in a)
        print(linea)
        if self.f:
            self.f.write(linea + "\n")
            self.f.flush()


def titulo(log, texto):
    log("\n" + "=" * 78)
    log(" " + texto)
    log("=" * 78)


def codigo(e):
    return e.response.get("Error", {}).get("Code", "?")


def archivos_locales():
    out = []
    for raiz, _, nombres in os.walk(RAW_LOCAL):
        for n in sorted(nombres):
            ruta = os.path.join(raiz, n)
            rel = os.path.relpath(ruta, RAW_LOCAL).replace(os.sep, "/")
            out.append((REMAPEO.get(rel, rel), ruta, os.path.getsize(ruta)))
    return sorted(out)


def objetos_s3(s3):
    objs = {}
    for pag in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIJO):
        for o in pag.get("Contents", []):
            if not o["Key"].endswith("/"):
                objs[o["Key"]] = o["Size"]
    return objs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aplicar", action="store_true", help="aplica cambios (sin esto: solo plan)")
    aplicar = ap.parse_args().aplicar
    log = Tee(EVIDENCIA if aplicar else None)
    ses = boto3.session.Session(region_name=REGION)
    s3 = ses.client("s3")
    modo = "APLICAR" if aplicar else "PLAN (solo lectura, no se modifica nada)"

    # ---------------------------------------------------------------- a. identidad
    titulo(log, "a. IDENTIDAD (sts get-caller-identity) - modo %s" % modo)
    ident = ses.client("sts").get_caller_identity()
    log("Cuenta : %s" % ident["Account"])
    log("ARN    : %s" % ident["Arn"])
    log("Region : %s | Bucket: s3://%s" % (REGION, BUCKET))

    # ---------------------------------------------------------------- b. seguridad del bucket
    titulo(log, "b. SEGURIDAD Y ETIQUETAS DEL BUCKET")
    try:
        actual = s3.get_public_access_block(Bucket=BUCKET)["PublicAccessBlockConfiguration"]
    except ClientError as e:
        actual = {} if codigo(e) == "NoSuchPublicAccessBlockConfiguration" else None
        if actual is None:
            raise
    try:
        reglas = s3.get_bucket_encryption(Bucket=BUCKET)["ServerSideEncryptionConfiguration"]["Rules"]
        cifrado = reglas[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    except ClientError as e:
        if codigo(e) != "ServerSideEncryptionConfigurationNotFoundError":
            raise
        cifrado = "ninguno"
    versionado = s3.get_bucket_versioning(Bucket=BUCKET).get("Status", "Disabled")
    try:
        tags = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket=BUCKET)["TagSet"]}
    except ClientError as e:
        if codigo(e) != "NoSuchTagSet":
            raise
        tags = {}

    cambios = []
    if actual != BLOQUEO:
        cambios.append(("Bloqueo de acceso publico", str(actual), "las 4 opciones = True",
                        lambda: s3.put_public_access_block(Bucket=BUCKET, PublicAccessBlockConfiguration=BLOQUEO)))
    if cifrado != "AES256":
        cambios.append(("Cifrado por defecto", cifrado, "SSE-S3 (AES256)",
                        lambda: s3.put_bucket_encryption(Bucket=BUCKET, ServerSideEncryptionConfiguration={
                            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                                       "BucketKeyEnabled": True}]})))
    if versionado != "Enabled":
        cambios.append(("Versionado", versionado, "Enabled",
                        lambda: s3.put_bucket_versioning(Bucket=BUCKET,
                                                         VersioningConfiguration={"Status": "Enabled"})))
    nuevas = dict(tags, **TAGS)                      # conserva etiquetas existentes
    if nuevas != tags:
        cambios.append(("Etiquetas", str(tags or "ninguna"), str(TAGS),
                        lambda: s3.put_bucket_tagging(Bucket=BUCKET, Tagging={
                            "TagSet": [{"Key": k, "Value": v} for k, v in sorted(nuevas.items())]})))

    log("%-28s %-34s %s" % ("control", "estado actual", "objetivo"))
    log("%-28s %-34s %s" % ("Bloqueo de acceso publico", "completo" if actual == BLOQUEO else "INCOMPLETO",
                            "las 4 opciones = True"))
    log("%-28s %-34s %s" % ("Cifrado por defecto", cifrado, "AES256 (SSE-S3)"))
    log("%-28s %-34s %s" % ("Versionado", versionado, "Enabled"))
    log("%-28s %-34s %s" % ("Etiquetas", tags or "ninguna", TAGS))
    if not cambios:
        log("Sin cambios: el bucket ya cumple.")
    for nombre, antes, despues, fn in cambios:
        if aplicar:
            fn()
            log("APLICADO  %s: %s -> %s" % (nombre, antes, despues))
        else:
            log("CAMBIARIA %s: %s -> %s" % (nombre, antes, despues))
    if aplicar and cambios:
        v = s3.get_bucket_versioning(Bucket=BUCKET).get("Status")
        tg = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket=BUCKET)["TagSet"]}
        pab = s3.get_public_access_block(Bucket=BUCKET)["PublicAccessBlockConfiguration"]
        enc = s3.get_bucket_encryption(Bucket=BUCKET)["ServerSideEncryptionConfiguration"]["Rules"][0]
        log("Verificacion: bloqueo=%s | cifrado=%s | versionado=%s | etiquetas=%s"
            % (all(pab.values()), enc["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"], v, tg))

    # ---------------------------------------------------------------- c. subida
    titulo(log, "c. SUBIDA data/raw/ -> s3://%s/%s (omite mismo tamano)" % (BUCKET, PREFIJO))
    locales = archivos_locales()
    remotos = objetos_s3(s3)
    subidos = omitidos = 0
    for rel, ruta, tam in locales:
        clave = PREFIJO + rel
        if remotos.get(clave) == tam:
            log("OMITIDO  %-50s %9.2f MB (ya existe, mismo tamano)" % (clave, tam / MB))
            omitidos += 1
            continue
        accion = "SUBIDO  " if aplicar else "SUBIRIA "
        if aplicar:
            s3.upload_file(ruta, BUCKET, clave)
        log("%s %-50s %9.2f MB%s" % (accion, clave, tam / MB,
                                     " (reemplaza: tamano distinto)" if clave in remotos else ""))
        subidos += 1
    log("Resumen: %d %s, %d omitidos, %d locales"
        % (subidos, "subidos" if aplicar else "por subir", omitidos, len(locales)))

    # ---------------------------------------------------------------- d. listado y comparacion
    titulo(log, "d. LISTADO s3://%s/%s vs data/raw/ local" % (BUCKET, PREFIJO))
    remotos = objetos_s3(s3)
    log("%-50s %12s %12s %s" % ("ruta S3", "S3 (MB)", "local (MB)", "estado"))
    loc = {PREFIJO + rel: tam for rel, _, tam in locales}
    for clave in sorted(set(loc) | set(remotos)):
        r, l_ = remotos.get(clave), loc.get(clave)
        estado = "OK" if r == l_ else ("FALTA EN S3" if r is None else ("SOLO EN S3" if l_ is None else "TAMANO DISTINTO"))
        log("%-50s %12s %12s %s" % (clave, "-" if r is None else "%.2f" % (r / MB),
                                     "-" if l_ is None else "%.2f" % (l_ / MB), estado))
    ok = sum(1 for k in loc if remotos.get(k) == loc[k])
    log("Archivos: S3=%d | local=%d | coinciden=%d | Total S3=%.2f MB | Total local=%.2f MB"
        % (len(remotos), len(loc), ok, sum(remotos.values()) / MB, sum(loc.values()) / MB))

    # ---------------------------------------------------------------- e. solo lectura de servicios
    titulo(log, "e. COMPROBACION DE ACCESO (solo lectura, no crea nada)")
    pruebas = [("emr-serverless list-applications",
                lambda: "%d aplicaciones" % len(ses.client("emr-serverless").list_applications(maxResults=50)["applications"])),
               ("glue get-databases",
                lambda: "%d bases: %s" % ((lambda d: (len(d), [x["Name"] for x in d]))(
                    ses.client("glue").get_databases(MaxResults=50)["DatabaseList"]))),
               ("athena list-work-groups",
                lambda: "workgroups: %s" % [w["Name"] for w in ses.client("athena").list_work_groups()["WorkGroups"]])]
    for nombre, fn in pruebas:
        try:
            log("%-36s RESPONDE        %s" % (nombre, fn()))
        except ClientError as e:
            log("%-36s ERROR %-10s %s" % (nombre, codigo(e), "sin permiso IAM" if "Denied" in codigo(e) else ""))
    if aplicar:
        log("\nEvidencia: evidencias/aws_01_s3.txt (sin claves: solo cuenta y ARN).")


if __name__ == "__main__":
    sys.exit(main())
