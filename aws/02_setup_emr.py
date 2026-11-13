"""AWS paso 2: EMR Serverless y presupuesto por codigo (boto3).

Requisitos (los aplica la cuenta raiz, ver aws/politicas/):
  * politica_usuario_emr.json adjunta al usuario proyecto-bigdata.
  * Rol sedalib-emr-execution-role con rol_ejecucion_emr_confianza.json (confianza) y
    rol_ejecucion_emr_permisos.json (permisos).

Uso (desde la raiz del proyecto; este script NO lee ni imprime claves):
    python aws/02_setup_emr.py              # --plan (por defecto): solo lectura, muestra que haria
    SEDALIB_BUDGET_EMAIL=correo@dominio python aws/02_setup_emr.py --aplicar
                                            # crea lo que falte y escribe evidencias/aws_02_emr.txt

a. Identidad (solo cuenta y ARN).
b. Verificacion de acceso: rol de ejecucion, emr-serverless, budgets.
c. Presupuesto AWS Budgets "sedalib-10usd": 10 USD/mes (costo), avisos por correo al 50 % y 80 %
   del gasto real. El correo se toma de SEDALIB_BUDGET_EMAIL y se enmascara en la salida.
d. Aplicacion EMR Serverless "sedalib-spark": Spark, version RELEASE (cambiar con --release), capacidad
   maxima 8 vCPU / 32 GB, sin capacidad preinicializada, autoinicio, autoapagado a los 5 minutos.
Idempotente: si el presupuesto o la aplicacion ya existen, no los vuelve a crear.
"""
import argparse
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
CUENTA = "856152566912"
ROL = "sedalib-emr-execution-role"
APP = "sedalib-spark"
RELEASE = "emr-7.9.0"         # EMR Serverless no tiene API para listar versiones; si no es valida, CreateApplication la rechaza
PRESUPUESTO = "sedalib-10usd"
LIMITE_USD = "10"
UMBRALES = (50, 80)
CAPACIDAD = {"cpu": "8 vCPU", "memory": "32 GB"}
APAGADO_MIN = 5
TAGS = {"Proyecto": "SEDALIB", "Curso": "BigData-UPAO"}
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVIDENCIA = os.path.join(BASE, "evidencias", "aws_02_emr.txt")


class Tee:
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


def enmascarar(correo):
    if not correo or "@" not in correo:
        return "(no definido)"
    u, d = correo.split("@", 1)
    return "%s***@%s" % (u[:1], d)


def probar(log, nombre, fn):
    try:
        res = fn()
        log("%-34s OK      %s" % (nombre, res))
        return True
    except ClientError as e:
        log("%-34s ERROR   %s" % (nombre, codigo(e)))
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aplicar", action="store_true", help="crea lo que falte (sin esto: solo plan)")
    ap.add_argument("--release", default=RELEASE, help="version EMR (por defecto %s)" % RELEASE)
    args = ap.parse_args()
    aplicar, release = args.aplicar, args.release
    log = Tee(EVIDENCIA if aplicar else None)
    ses = boto3.session.Session(region_name=REGION)
    emr = ses.client("emr-serverless")
    iam = ses.client("iam")
    bud = ses.client("budgets", region_name="us-east-1")       # Budgets es global (us-east-1)
    correo = os.environ.get("SEDALIB_BUDGET_EMAIL", "").strip()

    titulo(log, "a. IDENTIDAD - modo %s" % ("APLICAR" if aplicar else "PLAN (solo lectura, no se crea nada)"))
    ident = ses.client("sts").get_caller_identity()
    log("Cuenta : %s" % ident["Account"])
    log("ARN    : %s" % ident["Arn"])
    log("Region : %s" % REGION)

    # ---------------------------------------------------------------- b. verificacion
    titulo(log, "b. VERIFICACION DE ACCESO")
    estado = {}
    rol_arn = []
    estado["rol"] = probar(log, "iam get-role %s" % ROL,
                           lambda: rol_arn.append(iam.get_role(RoleName=ROL)["Role"]["Arn"]) or rol_arn[0])
    estado["emr"] = probar(log, "emr-serverless list-applications",
                           lambda: "%d aplicaciones" % len(emr.list_applications(maxResults=50)["applications"]))
    estado["budgets"] = probar(log, "budgets describe-budgets",
                               lambda: "%d presupuestos" % len(bud.describe_budgets(AccountId=CUENTA).get("Budgets", [])))
    faltan = [k for k, v in estado.items() if not v]
    if faltan:
        log("\nFALTAN PERMISOS/RECURSOS: %s. Aplicar aws/politicas/ con la cuenta raiz y repetir." % faltan)
        return 1

    # ---------------------------------------------------------------- c. presupuesto
    titulo(log, "c. PRESUPUESTO AWS BUDGETS")
    existentes = {b["BudgetName"]: b for b in bud.describe_budgets(AccountId=CUENTA).get("Budgets", [])}
    if PRESUPUESTO in existentes:
        b = existentes[PRESUPUESTO]
        log("YA EXISTE  %s: %s %s/%s" % (PRESUPUESTO, b["BudgetLimit"]["Amount"], b["BudgetLimit"]["Unit"], b["TimeUnit"]))
    else:
        log("%s %s: %s USD mensual (COST), avisos ACTUAL > %s %% del limite, correo %s"
            % ("CREANDO  " if aplicar else "CREARIA  ", PRESUPUESTO, LIMITE_USD,
               " y ".join(str(u) for u in UMBRALES), enmascarar(correo)))
        if aplicar:
            if not correo:
                log("ERROR: defina SEDALIB_BUDGET_EMAIL para los avisos.")
                return 1
            bud.create_budget(
                AccountId=CUENTA,
                Budget={"BudgetName": PRESUPUESTO, "BudgetLimit": {"Amount": LIMITE_USD, "Unit": "USD"},
                        "TimeUnit": "MONTHLY", "BudgetType": "COST"},
                NotificationsWithSubscribers=[
                    {"Notification": {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN",
                                      "Threshold": float(u), "ThresholdType": "PERCENTAGE"},
                     "Subscribers": [{"SubscriptionType": "EMAIL", "Address": correo}]} for u in UMBRALES])
    if aplicar:
        b = bud.describe_budget(AccountId=CUENTA, BudgetName=PRESUPUESTO)["Budget"]
        notis = bud.describe_notifications_for_budget(AccountId=CUENTA, BudgetName=PRESUPUESTO)["Notifications"]
        log("Verificacion: %s | limite %s %s/%s | avisos: %s"
            % (b["BudgetName"], b["BudgetLimit"]["Amount"], b["BudgetLimit"]["Unit"], b["TimeUnit"],
               ", ".join("%s > %g %%" % (n["NotificationType"], n["Threshold"]) for n in notis)))

    # ---------------------------------------------------------------- d. aplicacion EMR Serverless
    titulo(log, "d. APLICACION EMR SERVERLESS")
    log("Version EMR: %s" % release)
    apps = [a for pag in emr.get_paginator("list_applications").paginate() for a in pag["applications"]
            if a["name"] == APP and a["state"] not in ("TERMINATED",)]
    if apps:
        app_id = apps[0]["id"]
        log("YA EXISTE  %s (id %s, estado %s)" % (APP, app_id, apps[0]["state"]))
    else:
        log("%s %s: SPARK %s, X86_64, capacidad maxima %s / %s, sin capacidad preinicializada, "
            "autoinicio, autoapagado %d min, etiquetas %s"
            % ("CREANDO  " if aplicar else "CREARIA  ", APP, release, CAPACIDAD["cpu"], CAPACIDAD["memory"],
               APAGADO_MIN, TAGS))
        app_id = None
        if aplicar:
            app_id = emr.create_application(
                name=APP, releaseLabel=release, type="SPARK", architecture="X86_64",
                maximumCapacity=CAPACIDAD,
                autoStartConfiguration={"enabled": True},
                autoStopConfiguration={"enabled": True, "idleTimeoutMinutes": APAGADO_MIN},
                tags=TAGS)["applicationId"]
            for _ in range(30):
                if emr.get_application(applicationId=app_id)["application"]["state"] in ("CREATED", "STARTED"):
                    break
                time.sleep(5)
    if app_id:
        a = emr.get_application(applicationId=app_id)["application"]
        log("Verificacion: %s | id %s | estado %s | %s | %s | max %s / %s | autoapagado %s min | etiquetas %s"
            % (a["name"], a["applicationId"], a["state"], a["type"], a["releaseLabel"],
               a["maximumCapacity"]["cpu"], a["maximumCapacity"]["memory"],
               a["autoStopConfiguration"].get("idleTimeoutMinutes"), a.get("tags", {})))
        log("Rol de ejecucion para los jobs: %s" % rol_arn[0])
    if aplicar:
        log("\nEvidencia: evidencias/aws_02_emr.txt (sin claves; correo enmascarado).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
