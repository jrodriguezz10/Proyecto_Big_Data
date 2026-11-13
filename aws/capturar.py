"""Capturas del paso 1 de AWS (Windows, mss + ctypes). Recorta SOLO la ventana objetivo.

Uso (desde la raiz del proyecto):
    python aws/capturar.py identidad   -> capturas/aws/cap_aws01_identidad.png
    python aws/capturar.py subida      -> capturas/aws/cap_aws01_subida.png
    python aws/capturar.py bucket      -> capturas/aws/cap_aws01_bucket.png
    python aws/capturar.py permisos    -> capturas/aws/cap_aws01_seguridad.png
    python aws/capturar.py propiedades -> capturas/aws/cap_aws01_seguridad_propiedades.png
    python aws/capturar.py rol         -> capturas/aws/cap_aws02_rol.png
    python aws/capturar.py emr         -> capturas/aws/cap_aws02_emr_app.png
    python aws/capturar.py budget      -> capturas/aws/cap_aws02_budget.png
Las terminales ejecutan "cls" antes del comando. Nunca se muestran claves: sts se consulta con
--query para mostrar solo cuenta y ARN. Revisar cada PNG antes de versionarlo.
"""
import ctypes
import os
import subprocess
import sys
import time
import webbrowser

import mss
import mss.tools

BUCKET = "bigdata-lalibertad-jheyson-856152566912"
REGION = "us-east-2"
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEST = os.path.join(BASE, "capturas", "aws")
CONSOLA = "https://s3.console.aws.amazon.com/s3/buckets/%s?region=%s" % (BUCKET, REGION)
TITULO = "CAP_AWS01"
ROL_EMR, APP_EMR, PRESUPUESTO = "sedalib-emr-execution-role", "sedalib-spark", "sedalib-10usd"
IAM_ROL = "https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/%s?section=permissions" % ROL_EMR
EMR_APPS = "https://%s.console.aws.amazon.com/emr/home?region=%s#/serverless" % (REGION, REGION)
BUDGETS = "https://us-east-1.console.aws.amazon.com/billing/home#/budgets"

user32 = ctypes.windll.user32
user32.SetProcessDPIAware()


class RECT(ctypes.Structure):
    _fields_ = [("left", ctypes.c_long), ("top", ctypes.c_long), ("right", ctypes.c_long), ("bottom", ctypes.c_long)]


def capturar_ventana(hwnd, nombre):
    if user32.IsIconic(hwnd):
        user32.ShowWindow(hwnd, 9)             # SW_RESTORE solo si esta minimizada
    user32.SetForegroundWindow(hwnd)
    time.sleep(1.0)
    r = RECT()                                 # limites visibles (sin el borde invisible de Windows 11)
    if ctypes.windll.dwmapi.DwmGetWindowAttribute(hwnd, 9, ctypes.byref(r), ctypes.sizeof(r)) != 0:
        user32.GetWindowRect(hwnd, ctypes.byref(r))
    zona = {"left": r.left, "top": r.top, "width": r.right - r.left, "height": r.bottom - r.top}
    os.makedirs(DEST, exist_ok=True)
    ruta = os.path.join(DEST, nombre)
    with (mss.MSS() if hasattr(mss, "MSS") else mss.mss()) as sct:
        img = sct.grab(zona)
        mss.tools.to_png(img.rgb, img.size, output=ruta)
    print("Guardado: %s (%dx%d)" % (os.path.relpath(ruta, BASE), zona["width"], zona["height"]))


def buscar_ventana(texto, timeout=20):
    """hwnd de la primera ventana visible cuyo titulo contiene texto (reintenta hasta timeout s)."""
    proto = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)
    fin = time.time() + timeout
    while time.time() < fin:
        hallado = []

        def cb(hwnd, _):
            n = user32.GetWindowTextLengthW(hwnd)
            if n and user32.IsWindowVisible(hwnd):
                buf = ctypes.create_unicode_buffer(n + 1)
                user32.GetWindowTextW(hwnd, buf, n + 1)
                if texto in buf.value:
                    hallado.append(hwnd)
            return True

        user32.EnumWindows(proto(cb), 0)
        if hallado:
            return hallado[0]
        time.sleep(1)
    return None


def cerrar_sobrantes():
    while True:
        hwnd = buscar_ventana(TITULO, timeout=1)
        if not hwnd:
            return
        user32.PostMessageW(hwnd, 0x0010, 0, 0)    # WM_CLOSE
        time.sleep(1.5)


def terminal(comando, nombre, espera=10):
    """Abre una consola nueva con titulo fijo, ejecuta cls + comando, captura y la cierra."""
    cerrar_sobrantes()
    subprocess.Popen('start "%s" cmd /k "cls && %s"' % (TITULO, comando), shell=True, cwd=BASE).wait()
    time.sleep(espera)
    hwnd = buscar_ventana(TITULO)
    if not hwnd:
        sys.exit("No se encontro la ventana de consola '%s'" % TITULO)
    capturar_ventana(hwnd, nombre)
    user32.PostMessageW(hwnd, 0x0010, 0, 0)    # WM_CLOSE


def titulo_ventana(hwnd):
    n = user32.GetWindowTextLengthW(hwnd)
    buf = ctypes.create_unicode_buffer(n + 1)
    user32.GetWindowTextW(hwnd, buf, n + 1)
    return buf.value


def navegador(url, nombre, contiene=(BUCKET,), espera=8, maximo=45, pintar=4):
    """Abre la URL, espera >= espera s y hasta que el titulo de la pestana activa contenga alguno de
    `contiene` (pasada la redireccion de signin); maximiza la ventana y captura. Si no carga, no guarda."""
    webbrowser.open(url)
    time.sleep(espera)
    fin = time.time() + maximo
    ok = lambda h: any(c in titulo_ventana(h) for c in contiene) and "signin" not in titulo_ventana(h).lower()
    hwnd = user32.GetForegroundWindow()
    while not ok(hwnd) and time.time() < fin:
        time.sleep(2)
        hwnd = user32.GetForegroundWindow()
    if not ok(hwnd):
        sys.exit("La consola no cargo la pagina esperada (titulo: %r). Inicia sesion y repite." % titulo_ventana(hwnd))
    user32.ShowWindow(hwnd, 3)                 # SW_MAXIMIZE
    time.sleep(pintar)                         # termina de pintar el contenido de la pestana
    capturar_ventana(hwnd, nombre)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    op = sys.argv[1]
    if op == "identidad":
        terminal('title %s && aws sts get-caller-identity --query "{Cuenta:Account,ARN:Arn}" --output table'
                 % TITULO, "cap_aws01_identidad.png")
    elif op == "subida":
        terminal("title %s && aws s3 ls s3://%s/raw/ --recursive --human-readable --summarize --region %s"
                 % (TITULO, BUCKET, REGION), "cap_aws01_subida.png")
    elif op == "bucket":
        navegador(CONSOLA, "cap_aws01_bucket.png")
    elif op == "permisos":
        navegador(CONSOLA + "&tab=permissions", "cap_aws01_seguridad.png")
    elif op == "propiedades":
        navegador(CONSOLA + "&tab=properties", "cap_aws01_seguridad_propiedades.png")
    elif op == "rol":
        navegador(IAM_ROL, "cap_aws02_rol.png", contiene=(ROL_EMR, "IAM"), pintar=6)
    elif op == "emr":
        navegador(EMR_APPS, "cap_aws02_emr_app.png", contiene=(APP_EMR, "EMR"), pintar=8)
    elif op == "budget":
        navegador(BUDGETS, "cap_aws02_budget.png", contiene=(PRESUPUESTO, "Budget", "Presupuesto", "Billing", "Facturaci"),
                  pintar=8)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
