"""Paso 7: URL fija HTTPS para el dashboard (IP elastica + CloudFront) en us-east-2.

    python aws/07_cloudfront.py --plan      verificaciones (plan de la cuenta, permisos, DryRun) + plan + costo; no crea nada
    python aws/07_cloudfront.py --aplicar   crea/ajusta todo, enciende EC2, prueba HTTPS 200 y escribe evidencias/aws_07_sistema.txt

Que hace --aplicar (la instancia del paso 6 debe existir; se aprovecha que esta detenida):
 1. IP elastica sedalib-dashboard-eip (Proyecto=SEDALIB, Curso=BigData-UPAO) asociada a la instancia sedalib-dashboard.
 2. user-data por arranque (instancia detenida): en cada inicio git pull del repo publico + pip + restart del servicio,
    asi "sistema.py --encender --actualizar" publica datos nuevos sin SSH ni claves.
 3. CloudFront (Proyecto=SEDALIB): origen = DNS publico de la IP elastica, HTTP 8501; viewer redirect-to-https;
    cache policy Managed-CachingDisabled; origin request policy Managed-AllViewer (Streamlit usa WebSockets);
    todos los metodos HTTP; PriceClass_100; certificado por defecto *.cloudfront.net.
 4. Security group sedalib-dashboard-sg: agrega 8501 desde la prefix list com.amazonaws.global.cloudfront.origin-facing
    y quita 0.0.0.0/0 (el puerto 8501 deja de ser accesible directamente desde Internet).
 5. Enciende EC2, espera la distribucion Deployed y https://<dominio>.cloudfront.net -> HTTP 200; evidencia.
Si el plan de la cuenta bloquea CloudFront o la IP elastica, se detiene y muestra el error exacto (sin alternativas).
"""
import argparse
import datetime as dt
import sys
import time

from botocore.exceptions import ClientError

import sistema as S

CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"     # Managed-CachingDisabled
ALL_VIEWER = "216adef6-5c7f-47e4-b989-5492eafa07d3"           # Managed-AllViewer
PREFIX_CF = "com.amazonaws.global.cloudfront.origin-facing"
ETIQUETAS = [{"Key": "Proyecto", "Value": "SEDALIB"}, {"Key": "Curso", "Value": "BigData-UPAO"}]
EVID = S.os.path.join(S.BASE, "evidencias", "aws_07_sistema.txt")
ec2, cf = S.ec2, S.cf

USER_DATA_POR_ARRANQUE = """Content-Type: multipart/mixed; boundary="SEDALIB"
MIME-Version: 1.0

--SEDALIB
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--SEDALIB
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash
# Cada arranque: trae la ultima version del repo publico (datos del dashboard) y reinicia el servicio.
if [ -d /opt/sedalib/app/.git ]; then
  sudo -u sedalib git -C /opt/sedalib/app pull --ff-only || true
  sudo -u sedalib /opt/sedalib/venv/bin/pip install --no-cache-dir -q -r /opt/sedalib/app/dashboard/requirements.txt || true
  systemctl restart sedalib-dashboard
fi
--SEDALIB--
"""


def error(paso, ex):
    e = ex.response["Error"]
    msg = str(e.get("Message"))
    cuenta = S.boto3.client("sts").get_caller_identity()["Account"]
    sys.exit("DETENIDO en %s: %s - %s" % (paso, e.get("Code"), msg.replace(cuenta, "<CUENTA>")))


def prefix_list():
    r = ec2.describe_managed_prefix_lists(Filters=[{"Name": "prefix-list-name", "Values": [PREFIX_CF]}])["PrefixLists"]
    if not r:
        sys.exit("No se encontro la prefix list %s" % PREFIX_CF)
    return r[0]


def verificar():
    """Verificaciones de solo lectura. Devuelve dict con lo encontrado; sale con el error exacto si algo bloquea."""
    out = {}
    try:
        out["plan"] = S.boto3.client("freetier", region_name="us-east-1").get_account_plan_state()
    except ClientError as ex:
        out["plan"] = {"error": "%s - %s" % (ex.response["Error"]["Code"], ex.response["Error"]["Message"])}
    try:
        ec2.allocate_address(Domain="vpc", DryRun=True, TagSpecifications=[
            {"ResourceType": "elastic-ip", "Tags": ETIQUETAS + [{"Key": "Name", "Value": S.EIP}]}])
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "DryRunOperation":
            error("IP elastica (AllocateAddress DryRun)", ex)
        out["eip_dryrun"] = "DryRunOperation: la solicitud habria sido aceptada"
    try:
        cf.list_distributions(MaxItems="1")
        out["cache"] = cf.get_cache_policy(Id=CACHING_DISABLED)["CachePolicy"]["CachePolicyConfig"]["Name"]
        out["orp"] = cf.get_origin_request_policy(Id=ALL_VIEWER)["OriginRequestPolicy"]["OriginRequestPolicyConfig"]["Name"]
    except ClientError as ex:
        error("CloudFront (lectura)", ex)
    out["prefix"] = prefix_list()
    out["inst"] = S.instancia()
    out["sg"] = S.grupo()
    out["eip"] = S.ip_elastica()
    out["dist"] = S.distribucion()
    if not out["inst"] or not out["sg"]:
        sys.exit("Falta la instancia o el security group del paso 6.")
    return out


def costo(p):
    on, _ = S.costo_hora(p, "running", True)
    off, _ = S.costo_hora(p, "stopped", True)
    return on, off


def plan():
    v = verificar()
    p = S.precios()
    on, off = costo(p)
    plan_cuenta = v["plan"]
    print("VERIFICACIONES (solo lectura)")
    if "error" in plan_cuenta:
        print("  plan de la cuenta     : no se pudo leer (%s)" % plan_cuenta["error"])
    else:
        print("  plan de la cuenta     : %s | estado %s | creditos restantes %s"
              % (plan_cuenta.get("accountPlanType"), plan_cuenta.get("accountPlanStatus"),
                 plan_cuenta.get("accountPlanRemainingCredits", {}).get("amount", "-")))
    print("  IP elastica           : %s" % v["eip_dryrun"])
    print("  CloudFront            : API accesible; %s y %s existen" % (v["cache"], v["orp"]))
    print("                          (CloudFront no tiene DryRun: si el plan bloquea la creacion, --aplicar se detiene")
    print("                           con el error exacto antes de tocar el security group)")
    print("  prefix list           : %s (%s, administrada por %s)" % (PREFIX_CF, S.mask(v["prefix"]["PrefixListId"]),
                                                                 v["prefix"].get("OwnerId")))
    print("  instancia             : %s %s | SG %s" % (S.NOMBRE, v["inst"]["State"]["Name"], S.SG))
    print("  ya existen            : IP elastica %s | CloudFront %s\n" % ("si" if v["eip"] else "no",
                                                                        "si" if v["dist"] else "no"))
    print("PLAN (python aws/07_cloudfront.py --aplicar)")
    print(" 1. IP elastica %s (Proyecto=SEDALIB, Curso=BigData-UPAO) -> asociada a %s" % (S.EIP, S.NOMBRE))
    print(" 2. user-data por arranque en la instancia detenida: git pull + pip + restart del servicio en cada inicio")
    print(" 3. CloudFront 'SEDALIB dashboard' (Proyecto=SEDALIB): origen ec2-<IP-elastica>.%s.compute.amazonaws.com:%d"
          " HTTP;" % (S.REGION, S.PUERTO))
    print("    redirect-to-https, CachingDisabled, AllViewer (WebSockets), todos los metodos, PriceClass_100, HTTP/2, IPv6")
    print(" 4. %s: + tcp %d desde %s ; - tcp %d desde 0.0.0.0/0" % (S.SG, S.PUERTO, PREFIX_CF, S.PUERTO))
    print(" 5. enciende EC2, espera CloudFront Deployed (5-15 min) y https://<dominio>.cloudfront.net -> 200,")
    print("    evidencia + capturas, commit/push y al final sistema.py --apagar\n")
    print("COSTO (Pricing API; %s)" % S.UBICACION)
    print("  %-48s %8.4f USD/h" % ("instancia t3.micro", p["t3micro"]))
    print("  %-48s %8.4f USD/h" % ("IP elastica en uso (EC2 encendida)", p["ipv4_uso"]))
    print("  %-48s %8.4f USD/h" % ("IP elastica sin uso (EC2 detenida)", p["ipv4_ocio"]))
    print("  %-48s %8.4f USD/h" % ("disco gp3 %d GB" % S.DISCO_GB, p["disco_h"]))
    print("  %-48s %s USD/GB salida + %.4f USD/10k HTTPS (sin cargo fijo)" % ("CloudFront", p["cf_gb"], p["cf_10k"]))
    print("  %-48s %8.4f USD/h | %.4f USD/dia" % ("TOTAL ENCENDIDO (sin trafico CloudFront)", on, 24 * on))
    print("  %-48s %8.4f USD/h | %.4f USD/dia" % ("TOTAL APAGADO (--apagar)", off, 24 * off))
    print("  Diferencia con el paso 6: apagado sube de %.4f a %.4f USD/h porque la IP elastica cobra aunque EC2 este"
          " detenida." % (p["disco_h"], off))


def aplicar():
    v = verificar()
    i = v["inst"]
    if i["State"]["Name"] not in ("stopped", "running"):
        sys.exit("La instancia esta %s: esperar y repetir." % i["State"]["Name"])
    # 1. IP elastica
    e = v["eip"]
    if not e:
        try:
            r = ec2.allocate_address(Domain="vpc", TagSpecifications=[
                {"ResourceType": "elastic-ip", "Tags": ETIQUETAS + [{"Key": "Name", "Value": S.EIP}]}])
        except ClientError as ex:
            error("IP elastica (AllocateAddress)", ex)
        e = {"AllocationId": r["AllocationId"], "PublicIp": r["PublicIp"]}
        print("IP elastica creada: %s" % e["PublicIp"])
    if e.get("InstanceId") != i["InstanceId"]:
        ec2.associate_address(AllocationId=e["AllocationId"], InstanceId=i["InstanceId"])
        print("IP elastica asociada a %s" % S.mask(i["InstanceId"]))
    dns = S.dns_publico(e["PublicIp"])
    # 2. user-data por arranque (requiere instancia detenida)
    if i["State"]["Name"] == "stopped":
        ec2.modify_instance_attribute(InstanceId=i["InstanceId"], UserData={"Value": USER_DATA_POR_ARRANQUE.encode()})
        print("user-data por arranque instalado (git pull + restart en cada inicio)")
    else:
        print("AVISO: la instancia esta encendida; no se cambia el user-data (detener y repetir para instalarlo)")
    # 3. CloudFront
    d = v["dist"]
    if not d:
        conf = {
            "CallerReference": "sedalib-%d" % int(time.time()), "Comment": S.CF_COMENTARIO, "Enabled": True,
            "Origins": {"Quantity": 1, "Items": [{
                "Id": "sedalib-ec2", "DomainName": dns,
                "CustomOriginConfig": {"HTTPPort": S.PUERTO, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                                       "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                                       "OriginReadTimeout": 60, "OriginKeepaliveTimeout": 5}}]},
            "DefaultCacheBehavior": {
                "TargetOriginId": "sedalib-ec2", "ViewerProtocolPolicy": "redirect-to-https",
                "AllowedMethods": {"Quantity": 7, "Items": ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"],
                                   "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
                "CachePolicyId": CACHING_DISABLED, "OriginRequestPolicyId": ALL_VIEWER, "Compress": True},
            "PriceClass": "PriceClass_100", "HttpVersion": "http2", "IsIPV6Enabled": True,
            "ViewerCertificate": {"CloudFrontDefaultCertificate": True}}
        try:
            r = cf.create_distribution_with_tags(DistributionConfigWithTags={
                "DistributionConfig": conf, "Tags": {"Items": ETIQUETAS}})
        except ClientError as ex:
            error("CloudFront (CreateDistributionWithTags)", ex)
        d = r["Distribution"]
        print("CloudFront creada: %s -> https://%s/" % (S.mask(d["Id"]), d["DomainName"]))
    # 4. security group: primero agrega CloudFront, luego quita 0.0.0.0/0
    sg = S.grupo()
    pl = v["prefix"]["PrefixListId"]
    tiene_pl = any(x.get("PrefixListId") == pl for p in sg["IpPermissions"] for x in p.get("PrefixListIds", []))
    if not tiene_pl:
        ec2.authorize_security_group_ingress(GroupId=sg["GroupId"], IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": S.PUERTO, "ToPort": S.PUERTO,
            "PrefixListIds": [{"PrefixListId": pl, "Description": "CloudFront origin-facing"}]}])
        print("SG: + tcp %d desde %s" % (S.PUERTO, PREFIX_CF))
    abierto = any(x.get("CidrIp") == "0.0.0.0/0" for p in sg["IpPermissions"] for x in p.get("IpRanges", []))
    if abierto:
        ec2.revoke_security_group_ingress(GroupId=sg["GroupId"], IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": S.PUERTO, "ToPort": S.PUERTO, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
        print("SG: - tcp %d desde 0.0.0.0/0" % S.PUERTO)
    # 5. encender, esperar despliegue y HTTPS
    print("esperando CloudFront Deployed (5-15 min)...")
    cf.get_waiter("distribution_deployed").wait(Id=d["Id"], WaiterConfig={"Delay": 30, "MaxAttempts": 60})
    url = S.encender(False)
    evidencia(url, e["PublicIp"], dns, d["Id"])


def evidencia(url, ip, dns, dist_id):
    p = S.precios()
    on, off = costo(p)
    cod_https = S.responde(url)
    cod_health = S.responde(url + "_stcore/health")
    cod_http = None
    try:
        import http.client
        c = http.client.HTTPConnection(url.split("/")[2], 80, timeout=10)
        c.request("GET", "/")
        r = c.getresponse()
        cod_http = "%d -> %s" % (r.status, r.getheader("Location"))
    except Exception as ex:  # noqa: BLE001
        cod_http = "error %s" % ex.__class__.__name__
    directo = S.responde("http://%s:%d/_stcore/health" % (ip, S.PUERTO), timeout=8)
    sg = S.grupo()
    reglas = ["tcp %s <- %s" % (q.get("FromPort"), ", ".join([x["CidrIp"] for x in q.get("IpRanges", [])]
                                                           + [PREFIX_CF for _ in q.get("PrefixListIds", [])]))
              for q in sg["IpPermissions"]]
    ahora = dt.datetime.now(S.TZ).strftime("%Y-%m-%d %H:%M:%S (UTC-05:00)")
    try:
        pc = S.boto3.client("freetier", region_name="us-east-1").get_account_plan_state()
        cuenta = "%s (%s), creditos restantes %s USD - no es plan gratuito: CloudFront e IP elastica sin bloqueo" % (
            pc.get("accountPlanType"), pc.get("accountPlanStatus"),
            pc.get("accountPlanRemainingCredits", {}).get("amount", "-"))
    except ClientError as ex:
        cuenta = "no disponible (%s)" % ex.response["Error"]["Code"]
    L = ["=" * 100, "PASO 7 - URL FIJA HTTPS (CloudFront + IP elastica) Y SCRIPT DEL SISTEMA (%s)" % S.REGION, "=" * 100,
         "fecha            : %s" % ahora,
         "plan de la cuenta: %s" % cuenta,
         "URL publica      : %s" % url,
         "prueba HTTPS     : GET %s -> HTTP %s | GET /_stcore/health -> HTTP %s" % (url, cod_https, cod_health),
         "prueba HTTP      : GET http://%s/ -> %s (redirect-to-https)" % (url.split("/")[2], cod_http),
         "acceso directo   : http://%s:%d -> %s (el SG solo admite CloudFront)" % (ip, S.PUERTO,
                                                                                 "RESPONDE (revisar SG)" if directo else "sin respuesta (bloqueado)"),
         "CloudFront       : %s | origen %s:%d HTTP | CachingDisabled + AllViewer | PriceClass_100 | Proyecto=SEDALIB"
         % (S.mask(dist_id), dns, S.PUERTO),
         "IP elastica      : %s (%s), Proyecto=SEDALIB" % (ip, S.EIP),
         "security group   : %s: %s" % (S.SG, "; ".join(reglas)),
         "instancia        : user-data por arranque (git pull + restart) para publicar datos sin SSH",
         "",
         "COSTO (Pricing API, %s)" % S.UBICACION,
         "  encendido : %.4f USD/h | %.4f USD/dia  (t3.micro %.4f + IP elastica %.4f + disco %.4f; CloudFront sin cargo fijo)"
         % (on, 24 * on, p["t3micro"], p["ipv4_uso"], p["disco_h"]),
         "  apagado   : %.4f USD/h | %.4f USD/dia  (IP elastica sin uso %.4f + disco %.4f)"
         % (off, 24 * off, p["ipv4_ocio"], p["disco_h"]),
         "  CloudFront: %s USD/GB salida + %.4f USD/10k HTTPS; capa gratuita de CloudFront 1 TB y 10 M solicitudes/mes"
         % (p["cf_gb"], p["cf_10k"]),
         "",
         "Operacion: python aws/sistema.py --estado | --encender [--actualizar] | --apagar | --destruir"]
    with open(EVID, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))
    print("\n-> evidencias/aws_07_sistema.txt")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--plan", action="store_true")
    g.add_argument("--aplicar", action="store_true")
    a = ap.parse_args()
    plan() if a.plan else aplicar()
