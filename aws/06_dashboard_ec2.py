"""Paso 6: dashboard Streamlit en Amazon EC2 (us-east-2) para la sustentacion. Todo por codigo.

    python aws/06_dashboard_ec2.py --plan       plan + costo por hora (Pricing API); no crea nada
    python aws/06_dashboard_ec2.py --aplicar    crea SG + instancia, espera http://<IP>:8501, escribe evidencia
    python aws/06_dashboard_ec2.py --estado     estado de la instancia, URL y costo acumulado
    python aws/06_dashboard_ec2.py --detener    detiene la instancia (deja de cobrar computo e IPv4; sigue el disco)
    python aws/06_dashboard_ec2.py --iniciar    vuelve a encender la instancia detenida (la IP publica CAMBIA)
    python aws/06_dashboard_ec2.py --terminar   termina la instancia y borra el security group

Seguridad
* Sin claves en la instancia: sin key pair (no hay SSH), sin rol IAM / instance profile, IMDSv2 obligatorio.
  El dashboard lee solo dashboard/data/*.csv del repositorio publico (sin boto3 ni credenciales).
* Security group sedalib-dashboard-sg: entrada SOLO TCP 8501 desde 0.0.0.0/0 (sin 22). Salida por defecto
  (la instancia necesita salir a dnf, GitHub y PyPI).
* Una sola instancia: --aplicar se niega si ya existe una con Proyecto=SEDALIB y Name=sedalib-dashboard que no
  este terminada (IAM no puede limitar la cantidad; lo limita este script).
* Permisos del usuario: aws/politicas/politica_usuario_ec2.json (sedalib-usuario-ec2).
* Salidas sin ID de cuenta; el ID de instancia se muestra enmascarado (i-...ABCD).
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.request

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
UBICACION = "US East (Ohio)"
TIPO = "t3.micro"
DISCO_GB = 8
NOMBRE = "sedalib-dashboard"
SG = "sedalib-dashboard-sg"
PUERTO = 8501
ETIQUETAS = [{"Key": "Proyecto", "Value": "SEDALIB"}, {"Key": "Curso", "Value": "BigData-UPAO"}]
AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
REPO = "https://github.com/jrodriguezz10/Proyecto_Big_Data.git"
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias", "aws_06_dashboard_ec2.txt")
HORAS_MES = 730

ses = boto3.session.Session(region_name=REGION)
ec2 = ses.client("ec2")

USER_DATA = r"""#!/bin/bash
# user-data del dashboard SEDALIB (Amazon Linux 2023). Log: /var/log/cloud-init-output.log
set -euxo pipefail
dnf install -y git python3.11 python3.11-pip          # AL2023 trae Python 3.9; pandas 3 necesita >= 3.11
# swap de 1 GB: t3.micro tiene 1 GB de RAM (pip + Streamlit + pandas)
dd if=/dev/zero of=/swapfile bs=1M count=1024
chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
useradd --system --create-home --home-dir /opt/sedalib --shell /sbin/nologin sedalib
sudo -u sedalib git clone --depth 1 __REPO__ /opt/sedalib/app
sudo -u sedalib python3.11 -m venv /opt/sedalib/venv
sudo -u sedalib /opt/sedalib/venv/bin/pip install --no-cache-dir -r /opt/sedalib/app/dashboard/requirements.txt
cat > /etc/systemd/system/sedalib-dashboard.service <<'UNIT'
[Unit]
Description=Dashboard SEDALIB (Streamlit)
After=network-online.target
Wants=network-online.target

[Service]
User=sedalib
WorkingDirectory=/opt/sedalib/app
ExecStart=/opt/sedalib/venv/bin/streamlit run dashboard/app.py --server.port __PUERTO__ --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now sedalib-dashboard
""".replace("__REPO__", REPO).replace("__PUERTO__", str(PUERTO))


# ---------------------------------------------------------------- utilidades
def mask(i):
    return "-" if not i else i[:2] + "..." + i[-4:]


def filtro_proyecto():
    return [{"Name": "tag:Proyecto", "Values": ["SEDALIB"]}, {"Name": "tag:Name", "Values": [NOMBRE]}]


def instancias(estados=("pending", "running", "stopping", "stopped", "shutting-down")):
    r = ec2.describe_instances(Filters=filtro_proyecto() + [{"Name": "instance-state-name", "Values": list(estados)}])
    return [i for res in r["Reservations"] for i in res["Instances"]]


def grupo():
    r = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG]},
                                              {"Name": "tag:Proyecto", "Values": ["SEDALIB"]}])["SecurityGroups"]
    return r[0] if r else None


def ami():
    p = ses.client("ssm").get_parameter(Name=AMI_PARAM)["Parameter"]["Value"]
    img = ec2.describe_images(ImageIds=[p])["Images"][0]
    return p, img["Name"], img["CreationDate"][:10]


def red():
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpc:
        sys.exit("No hay VPC por defecto en %s" % REGION)
    vpc_id = vpc[0]["VpcId"]
    subs = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]},
                                         {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]
    subs = sorted([s for s in subs if s.get("MapPublicIpOnLaunch")], key=lambda s: s["AvailabilityZone"])
    if not subs:
        sys.exit("La VPC por defecto no tiene subred publica por defecto")
    return vpc_id, subs[0]["SubnetId"], subs[0]["AvailabilityZone"]


def _precio(servicio, filtros):
    pr = boto3.client("pricing", region_name="us-east-1")
    f = [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filtros.items()]
    lst = pr.get_products(ServiceCode=servicio, Filters=f, MaxResults=20)["PriceList"]
    out = []
    for raw in lst:
        p = json.loads(raw)
        for term in p.get("terms", {}).get("OnDemand", {}).values():
            for dim in term["priceDimensions"].values():
                usd = float(dim["pricePerUnit"].get("USD", 0))
                if usd > 0:
                    out.append((usd, dim["unit"], dim["description"], p["product"]["attributes"].get("usagetype", "")))
    return out


def precios():
    inst = _precio("AmazonEC2", {"instanceType": TIPO, "location": UBICACION, "operatingSystem": "Linux",
                                 "tenancy": "Shared", "preInstalledSw": "NA", "capacitystatus": "Used",
                                 "licenseModel": "No License required"})
    ipv4 = [x for x in _precio("AmazonVPC", {"location": UBICACION, "usagetype": "USE2-PublicIPv4:InUseAddress"})
            if "InUseAddress" in x[3]]
    disco = _precio("AmazonEC2", {"location": UBICACION, "productFamily": "Storage", "volumeApiName": "gp3"})
    if len(inst) != 1 or not ipv4 or len(disco) != 1:
        sys.exit("Pricing API devolvio %d/%d/%d precios (instancia/IPv4/gp3): revisar filtros"
                 % (len(inst), len(ipv4), len(disco)))
    return {"instancia": inst[0], "ipv4": ipv4[0], "gp3": disco[0]}


def costo(pr):
    h_inst, h_ip = pr["instancia"][0], pr["ipv4"][0]
    h_disco = pr["gp3"][0] * DISCO_GB / HORAS_MES
    return h_inst, h_ip, h_disco, h_inst + h_ip + h_disco


def responde(ip, timeout=5):
    try:
        with urllib.request.urlopen("http://%s:%d/_stcore/health" % (ip, PUERTO), timeout=timeout) as r:
            return r.status == 200 and r.read().decode().strip() == "ok"
    except Exception:
        return False


def tabla_costos(pr):
    h_inst, h_ip, h_disco, h = costo(pr)
    L = ["Precios on-demand %s (AWS Pricing API, consultados %s UTC):" % (UBICACION, dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")),
         "  %-34s %10.4f USD/h   (%s)" % ("instancia %s Linux" % TIPO, h_inst, pr["instancia"][2]),
         "  %-34s %10.4f USD/h   (%s)" % ("IPv4 publica en uso", h_ip, pr["ipv4"][2]),
         "  %-34s %10.4f USD/h   (%.4f USD/GB-mes x %d GB / %d h)" % ("disco gp3 %d GB" % DISCO_GB, h_disco,
                                                                    pr["gp3"][0], DISCO_GB, HORAS_MES),
         "  %-34s %10.4f USD/h | %.4f USD/dia | %.2f USD/mes (730 h)" % ("TOTAL encendida", h, 24 * h, HORAS_MES * h),
         "  %-34s %10.4f USD/h | %.4f USD/dia (solo disco; la IPv4 publica se libera al detener)"
         % ("detenida (--detener)", h_disco, 24 * h_disco)]
    return L


# ---------------------------------------------------------------- modos
def plan():
    a, a_nom, a_fecha = ami()
    vpc, sub, az = red()
    sg, vivas = grupo(), instancias()
    pr = precios()
    print("PLAN paso 6 - dashboard en EC2 (%s). No se crea nada en este modo.\n" % REGION)
    print("1. Security group %s en la VPC por defecto (%s)" % (SG, "ya existe: se reutiliza" if sg else "nuevo"))
    print("     entrada : SOLO TCP %d desde 0.0.0.0/0 (sin SSH 22)" % PUERTO)
    print("     salida  : todo (por defecto; dnf, GitHub, PyPI)")
    print("     etiquetas: Proyecto=SEDALIB, Curso=BigData-UPAO, Name=%s" % SG)
    print("2. Instancia %s (%s) %s" % (NOMBRE, TIPO, "YA EXISTE: --aplicar se negara" if vivas else "nueva"))
    print("     AMI     : %s (%s, publicada %s, via SSM %s)" % (mask(a), a_nom, a_fecha, AMI_PARAM.split("/")[-1]))
    print("     red     : subred publica por defecto en %s, IPv4 publica automatica" % az)
    print("     disco   : gp3 %d GB, se borra al terminar" % DISCO_GB)
    print("     acceso  : SIN key pair (no hay SSH), SIN rol IAM, IMDSv2 obligatorio")
    print("     etiquetas: Proyecto=SEDALIB, Curso=BigData-UPAO, Name=%s (instancia y disco)" % NOMBRE)
    print("3. user-data: git + python3.11 + swap 1 GB; clona %s (publico, sin token);" % REPO)
    print("     venv + pip install -r dashboard/requirements.txt; servicio systemd sedalib-dashboard")
    print("     -> streamlit run dashboard/app.py --server.port %d --server.address 0.0.0.0 (arranca solo)" % PUERTO)
    print("4. Espera a que http://<IP>:%d/_stcore/health responda 'ok' (max. 20 min) y escribe la evidencia.\n" % PUERTO)
    print("\n".join(tabla_costos(pr)))


def aplicar():
    if instancias():
        sys.exit("Ya existe una instancia %s no terminada: no se crea otra (ver --estado)." % NOMBRE)
    a, a_nom, _ = ami()
    vpc, sub, az = red()
    pr = precios()
    sg = grupo()
    if sg:
        sg_id = sg["GroupId"]
        print("security group existente:", mask(sg_id))
    else:
        sg_id = ec2.create_security_group(
            GroupName=SG, Description="SEDALIB dashboard Streamlit: solo TCP 8501", VpcId=vpc,
            TagSpecifications=[{"ResourceType": "security-group", "Tags": ETIQUETAS + [{"Key": "Name", "Value": SG}]}])["GroupId"]
        ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": PUERTO, "ToPort": PUERTO,
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Streamlit dashboard"}]}])
        print("security group creado:", mask(sg_id))
    r = ec2.run_instances(
        ImageId=a, InstanceType=TIPO, MinCount=1, MaxCount=1, UserData=USER_DATA,
        NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": sub, "Groups": [sg_id], "AssociatePublicIpAddress": True,
                            "DeleteOnTermination": True}],
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {"VolumeSize": DISCO_GB, "VolumeType": "gp3",
                                                                  "DeleteOnTermination": True, "Encrypted": True}}],
        MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
        TagSpecifications=[{"ResourceType": t, "Tags": ETIQUETAS + [{"Key": "Name", "Value": NOMBRE}]}
                           for t in ("instance", "volume")])
    iid = r["Instances"][0]["InstanceId"]
    t0 = time.time()
    print("instancia lanzada: %s (%s) - esperando 'running'..." % (mask(iid), TIPO))
    ec2.get_waiter("instance_running").wait(InstanceIds=[iid])
    i = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
    ip = i.get("PublicIpAddress")
    print("running | IP publica %s | esperando el dashboard en http://%s:%d ..." % (ip, ip, PUERTO))
    t_ok = None
    while time.time() - t0 < 20 * 60:
        if responde(ip):
            t_ok = time.time()
            break
        time.sleep(15)
    if not t_ok:
        print("El dashboard no respondio en 20 min. Ultimas lineas de la consola de la instancia:")
        try:
            out = ec2.get_console_output(InstanceId=iid, Latest=True).get("Output", "")
            print("\n".join(out.splitlines()[-40:]))
        except ClientError as e:
            print(e)
        sys.exit("Detenido: no se escribe evidencia. La instancia sigue encendida (usar --terminar si corresponde).")
    print("dashboard OK en %.0f s desde el lanzamiento" % (t_ok - t0))
    evidencia(i, ip, pr, a_nom, az, t_ok - t0)


def evidencia(i, ip, pr, a_nom, az, seg):
    h_inst, h_ip, h_disco, h = costo(pr)
    with urllib.request.urlopen("http://%s:%d/" % (ip, PUERTO), timeout=10) as r:
        cod = r.status
    L = ["=" * 100, "PASO 6 - DASHBOARD EN AMAZON EC2 (%s)" % REGION, "=" * 100,
         "instancia      : %s  (Name=%s)" % (mask(i["InstanceId"]), NOMBRE),
         "tipo           : %s | AMI %s | zona %s" % (i["InstanceType"], a_nom, az),
         "inicio         : %s" % i["LaunchTime"].astimezone(dt.timezone(dt.timedelta(hours=-5))).strftime("%Y-%m-%d %H:%M:%S (UTC-05:00)"),
         "URL            : http://%s:%d/" % (ip, PUERTO),
         "verificacion   : GET /_stcore/health -> ok | GET / -> HTTP %d | listo %.0f s despues del lanzamiento" % (cod, seg),
         "acceso         : sin key pair, sin rol IAM, IMDSv2 obligatorio; SG %s: entrada solo TCP %d 0.0.0.0/0" % (SG, PUERTO),
         "disco          : gp3 %d GB cifrado, DeleteOnTermination" % DISCO_GB,
         "etiquetas      : Proyecto=SEDALIB, Curso=BigData-UPAO, Name=%s" % NOMBRE,
         "servicio       : systemd sedalib-dashboard -> streamlit run dashboard/app.py --server.port %d --server.address 0.0.0.0" % PUERTO,
         "", "COSTO ESTIMADO"] + tabla_costos(pr) + [
         "", "La instancia queda ENCENDIDA para la sustentacion. Apagar con: python aws/06_dashboard_ec2.py --terminar"]
    with open(EVID, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))
    print("\n-> evidencias/aws_06_dashboard_ec2.txt")


def estado():
    vivas = instancias(("pending", "running", "stopping", "stopped", "shutting-down"))
    print("Amazon EC2 (%s) - dashboard SEDALIB\n" % REGION)
    if not vivas:
        print("No hay instancia %s activa (terminada o no creada)." % NOMBRE)
        return
    i = vivas[0]
    ip = i.get("PublicIpAddress")
    tags = {t["Key"]: t["Value"] for t in i.get("Tags", [])}
    st = ec2.describe_instance_status(InstanceIds=[i["InstanceId"]], IncludeAllInstances=True)["InstanceStatuses"]
    chk = ("%s / %s" % (st[0]["SystemStatus"]["Status"], st[0]["InstanceStatus"]["Status"])) if st else "-"
    lanz = i["LaunchTime"]
    horas = (dt.datetime.now(dt.timezone.utc) - lanz).total_seconds() / 3600
    print("%-22s %s" % ("Instancia", mask(i["InstanceId"])))
    print("%-22s %s" % ("Name", tags.get("Name")))
    print("%-22s %s" % ("Estado", i["State"]["Name"]))
    print("%-22s %s" % ("Tipo", i["InstanceType"]))
    print("%-22s %s" % ("Zona", i["Placement"]["AvailabilityZone"]))
    print("%-22s %s" % ("Inicio", lanz.astimezone(dt.timezone(dt.timedelta(hours=-5))).strftime("%Y-%m-%d %H:%M:%S (UTC-05:00)")))
    print("%-22s %s" % ("Checks sistema/inst.", chk))
    print("%-22s %s" % ("Key pair", i.get("KeyName", "ninguno")))
    print("%-22s %s" % ("Rol IAM", i.get("IamInstanceProfile", {}).get("Arn", "ninguno").split("/")[-1]))
    print("%-22s %s" % ("IMDSv2", i.get("MetadataOptions", {}).get("HttpTokens")))
    print("%-22s %s" % ("Etiquetas", ", ".join("%s=%s" % kv for kv in sorted(tags.items()))))
    for g in i.get("SecurityGroups", []):
        sg = ec2.describe_security_groups(GroupIds=[g["GroupId"]])["SecurityGroups"][0]
        reglas = ["%s %s-%s %s" % (p["IpProtocol"], p.get("FromPort"), p.get("ToPort"),
                                   ",".join(x["CidrIp"] for x in p.get("IpRanges", []))) for p in sg["IpPermissions"]]
        print("%-22s %s: entrada %s" % ("Security group", sg["GroupName"], "; ".join(reglas)))
    if ip:
        print("%-22s http://%s:%d/  -> %s" % ("URL", ip, PUERTO, "responde (health ok)" if responde(ip) else "NO responde"))
    try:
        h = costo(precios())[3]
        print("%-22s %.4f USD/h | %.4f USD/dia | encendida %.1f h -> ~%.4f USD" % ("Costo estimado", h, 24 * h, horas, horas * h))
    except Exception as e:  # noqa: BLE001
        print("%-22s no disponible (%s)" % ("Costo estimado", e.__class__.__name__))


def detener():
    vivas = instancias(("pending", "running"))
    if not vivas:
        sys.exit("No hay instancia encendida.")
    iid = vivas[0]["InstanceId"]
    ec2.stop_instances(InstanceIds=[iid])
    ec2.get_waiter("instance_stopped").wait(InstanceIds=[iid])
    print("Instancia %s detenida (el disco gp3 sigue cobrando ~0.0009 USD/h)." % mask(iid))


def iniciar():
    paradas = instancias(("stopped",))
    if not paradas:
        sys.exit("No hay instancia detenida (ver --estado).")
    iid = paradas[0]["InstanceId"]
    t0 = time.time()
    ec2.start_instances(InstanceIds=[iid])
    print("iniciando %s ..." % mask(iid))
    ec2.get_waiter("instance_running").wait(InstanceIds=[iid])
    ip = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0].get("PublicIpAddress")
    print("running | IP publica NUEVA %s | esperando el dashboard (systemd lo arranca solo)..." % ip)
    while time.time() - t0 < 10 * 60:
        if responde(ip):
            print("dashboard OK en %.0f s -> http://%s:%d/" % (time.time() - t0, ip, PUERTO))
            with open(EVID, "a", encoding="utf-8") as f:
                f.write("reinicio %s: URL http://%s:%d/ (la IP cambia en cada --iniciar)\n"
                        % (dt.datetime.now(dt.timezone(dt.timedelta(hours=-5))).strftime("%Y-%m-%d %H:%M (UTC-05:00)"),
                           ip, PUERTO))
            return
        time.sleep(10)
    sys.exit("La instancia esta encendida pero el dashboard no respondio en 10 min (ver --estado).")


def terminar():
    vivas = instancias()
    for i in vivas:
        ec2.terminate_instances(InstanceIds=[i["InstanceId"]])
        print("terminando %s ..." % mask(i["InstanceId"]))
    if vivas:
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[i["InstanceId"] for i in vivas])
        print("instancia(s) terminada(s); disco borrado (DeleteOnTermination)")
    sg = grupo()
    if sg:
        for k in range(10):
            try:
                ec2.delete_security_group(GroupId=sg["GroupId"])
                print("security group %s borrado" % SG)
                break
            except ClientError as e:
                if "DependencyViolation" not in str(e) or k == 9:
                    raise
                time.sleep(15)
    print("Listo: no quedan recursos del paso 6.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    modos = {"plan": plan, "aplicar": aplicar, "estado": estado, "detener": detener, "iniciar": iniciar,
             "terminar": terminar}
    for m in modos:
        g.add_argument("--" + m, action="store_true")
    a = ap.parse_args()
    modos[next(m for m in modos if getattr(a, m))]()
