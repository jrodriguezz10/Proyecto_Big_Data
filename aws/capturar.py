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
    python aws/capturar.py jobs        -> capturas/aws/cap_aws03_jobs.png        (aws/ver_aws03.py)
    python aws/capturar.py salidas     -> capturas/aws/cap_aws03_s3_salidas.png
    python aws/capturar.py metricas    -> capturas/aws/cap_aws03_metricas.png
    python aws/capturar.py jobs04      -> capturas/aws/cap_aws04_jobs.png        (aws/ver_aws04.py)
    python aws/capturar.py replicas04  -> capturas/aws/cap_aws04_s3_replicas.png (antes de borrar)
    python aws/capturar.py glue05      -> capturas/aws/cap_aws05_glue_tablas.png  (aws/ver_aws05.py glue)
    python aws/capturar.py athena05    -> capturas/aws/cap_aws05_athena.png       (aws/ver_aws05.py athena)
    python aws/capturar.py dashboard   -> capturas/cap14_dashboard.png (Edge headless, perfil temporal;
                                          requiere "streamlit run dashboard/app.py" en el puerto 8501)
    python aws/capturar.py plan        -> capturas/cap14b_plan_accion.png (misma ventana, URL ?vista=plan)
    python aws/capturar.py ec2estado   -> capturas/aws/cap_aws06_instancia.png (06_dashboard_ec2.py --estado, PrintWindow)
    python aws/capturar.py ec2dash=http://<IP>:8501/ -> capturas/aws/cap_aws06_dashboard_ec2.png (Edge normal, perfil temporal)
    python aws/capturar.py cf07=https://<dominio>.cloudfront.net/ -> capturas/aws/cap_aws07_cloudfront.png
    python aws/capturar.py sistema07   -> capturas/aws/cap_aws07_sistema.png (aws/sistema.py --estado, PrintWindow)
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


def capturar_contenido(hwnd, ruta):
    """Captura SOLO el contenido de la ventana con PrintWindow(PW_RENDERFULLCONTENT): no lee pixeles de la
    pantalla, asi otras ventanas encima (videollamadas, notificaciones, barra de tareas) nunca salen."""
    gdi32 = ctypes.windll.gdi32
    r = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(r))
    w, h = r.right - r.left, r.bottom - r.top
    hdc_v = user32.GetWindowDC(hwnd)
    hdc = gdi32.CreateCompatibleDC(hdc_v)
    bmp = gdi32.CreateCompatibleBitmap(hdc_v, w, h)
    gdi32.SelectObject(hdc, bmp)
    ok = user32.PrintWindow(hwnd, hdc, 2)

    class BIH(ctypes.Structure):
        _fields_ = [("biSize", ctypes.c_uint32), ("biWidth", ctypes.c_int32), ("biHeight", ctypes.c_int32),
                    ("biPlanes", ctypes.c_uint16), ("biBitCount", ctypes.c_uint16), ("biCompression", ctypes.c_uint32),
                    ("biSizeImage", ctypes.c_uint32), ("x", ctypes.c_int32), ("y", ctypes.c_int32),
                    ("c1", ctypes.c_uint32), ("c2", ctypes.c_uint32)]
    bih = BIH(ctypes.sizeof(BIH), w, -h, 1, 32, 0, 0, 0, 0, 0, 0)
    buf = ctypes.create_string_buffer(w * h * 4)
    gdi32.GetDIBits(hdc, bmp, 0, h, buf, ctypes.byref(bih), 0)
    gdi32.DeleteObject(bmp); gdi32.DeleteDC(hdc); user32.ReleaseDC(hwnd, hdc_v)
    if not ok:
        sys.exit("PrintWindow fallo: no se guarda captura")
    bgra = buf.raw
    rgb = bytearray(w * h * 3)
    rgb[0::3], rgb[1::3], rgb[2::3] = bgra[2::4], bgra[1::4], bgra[0::4]
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    mss.tools.to_png(bytes(rgb), (w, h), output=ruta)
    print("Guardado: %s (%dx%d, PrintWindow)" % (os.path.relpath(ruta, BASE), w, h))


def dashboard(url="http://localhost:8501/", nombre="cap14_dashboard.png", escala="0.8", espera_ms=25000):
    """Captura el dashboard con Edge HEADLESS y PERFIL TEMPORAL: no abre ninguna ventana ni lee pixeles de la
    pantalla (nada del escritorio del usuario puede salir) y no usa el perfil, pestanas ni extensiones del
    usuario. Viewport 2400x1300 CSS a escala 0.8 -> PNG 1920x1040.
    (El metodo anterior, ventana --app + PrintWindow, salio oscurecido: Edge abria un dialogo propio en otra
    ventana y atenuaba la pagina.)"""
    import shutil
    import tempfile
    edge = next(p for p in (r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe") if os.path.exists(p))
    perfil = tempfile.mkdtemp(prefix="sedalib_cap_edge_")
    ruta = os.path.join(BASE, "capturas", nombre)
    try:
        subprocess.run([edge, "--headless=new", "--user-data-dir=" + perfil, "--no-first-run", "--disable-gpu",
                        "--hide-scrollbars", "--disable-notifications", "--lang=es", "--window-size=2400,1300",
                        "--force-device-scale-factor=" + escala, "--virtual-time-budget=%d" % espera_ms,
                        "--screenshot=" + ruta, url], check=True, capture_output=True, timeout=180)
    finally:
        shutil.rmtree(perfil, ignore_errors=True)
    print("Guardado: %s (Edge headless, escala %s)" % (os.path.relpath(ruta, BASE), escala))


def terminal_pw(comando, nombre, espera=15):
    """Como terminal(), pero captura con PrintWindow (contenido de la ventana, no pixeles de la pantalla)."""
    cerrar_sobrantes()
    subprocess.Popen('start "%s" cmd /k "cls && %s"' % (TITULO, comando), shell=True, cwd=BASE).wait()
    time.sleep(espera)
    hwnd = buscar_ventana(TITULO)
    if not hwnd:
        sys.exit("No se encontro la ventana de consola '%s'" % TITULO)
    capturar_contenido(hwnd, os.path.join(DEST, nombre))
    user32.PostMessageW(hwnd, 0x0010, 0, 0)


def navegador_url(url, nombre, titulo="SEDALIB - riesgo", espera=25):
    """Edge en ventana NORMAL (con barra de direccion visible) y PERFIL TEMPORAL: sin pestanas, extensiones
    ni sesion del usuario. Ventana fuera de pantalla, captura con PrintWindow (no lee la pantalla)."""
    import shutil
    import tempfile
    edge = next(p for p in (r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe") if os.path.exists(p))
    perfil = tempfile.mkdtemp(prefix="sedalib_cap_edge_")
    proc = subprocess.Popen([edge, "--user-data-dir=" + perfil, "--new-window", url, "--no-first-run",
                             "--no-default-browser-check", "--disable-notifications", "--disable-translate",
                             "--disable-sync", "--disable-extensions", "--hide-crash-restore-bubble",
                             "--disable-features=Translate,msEdgeTranslate,msUndersideButton,CalculateNativeWinOcclusion,"
                             "msImplicitSignin,msEdgeSidebarV2,msHubApps",
                             "--disable-backgrounding-occluded-windows", "--disable-renderer-backgrounding", "--lang=es",
                             "--force-device-scale-factor=0.8", "--window-position=-4000,0", "--window-size=1920,1040"])
    try:
        hwnd = buscar_ventana(titulo, timeout=90)
        if not hwnd:
            sys.exit("No se abrio la ventana del dashboard")
        for _ in range(3):
            user32.SetWindowPos(hwnd, 0, -4000, 0, 1920, 1040, 0x0014)
            time.sleep(1)
        time.sleep(espera)
        capturar_contenido(hwnd, os.path.join(DEST, nombre))
        user32.PostMessageW(hwnd, 0x0010, 0, 0)
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
    finally:
        time.sleep(2)
        shutil.rmtree(perfil, ignore_errors=True)


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
        # La consola solo lista aplicaciones dentro de EMR Studio (crearia un recurso): se usa la CLI.
        import boto3
        apps = boto3.client("emr-serverless", region_name=REGION).list_applications()["applications"]
        ids = [a["id"] for a in apps if a["name"] == APP_EMR and a["state"] != "TERMINATED"]
        if not ids:
            sys.exit("No existe la aplicacion %s" % APP_EMR)
        app_id = ids[0]
        terminal('title %s && aws emr-serverless get-application --region %s --application-id %s --output table '
                 '--query "application.{Nombre:name,Estado:state,Release:releaseLabel,CPU_max:maximumCapacity.cpu,'
                 'Memoria_max:maximumCapacity.memory,Autoapagado_min:autoStopConfiguration.idleTimeoutMinutes,'
                 'Etiquetas:tags}"' % (TITULO, REGION, app_id), "cap_aws02_emr_app.png")
    elif op in ("jobs", "salidas", "metricas"):
        nombre = {"jobs": "cap_aws03_jobs.png", "salidas": "cap_aws03_s3_salidas.png",
                  "metricas": "cap_aws03_metricas.png"}[op]
        terminal("title %s && python aws/ver_aws03.py %s" % (TITULO, op), nombre, espera=15)
    elif op in ("jobs04", "replicas04"):
        nombre = {"jobs04": "cap_aws04_jobs.png", "replicas04": "cap_aws04_s3_replicas.png"}[op]
        terminal("title %s && python aws/ver_aws04.py %s" % (TITULO, op[:-2]), nombre, espera=15)
    elif op in ("glue05", "athena05"):
        nombre = {"glue05": "cap_aws05_glue_tablas.png", "athena05": "cap_aws05_athena.png"}[op]
        terminal("title %s && python aws/ver_aws05.py %s" % (TITULO, op[:-2]), nombre, espera=15)
    elif op == "ec2estado":
        terminal_pw("title %s && python aws/06_dashboard_ec2.py --estado" % TITULO, "cap_aws06_instancia.png", espera=25)
    elif op == "sistema07":
        terminal_pw("title %s && python aws/sistema.py --estado" % TITULO, "cap_aws07_sistema.png", espera=30)
    elif op.startswith("cf07="):                    # cf07=https://<dominio>.cloudfront.net/
        navegador_url(op.split("=", 1)[1], "cap_aws07_cloudfront.png")
    elif op.startswith("ec2dash="):                 # ec2dash=http://<IP>:8501/
        navegador_url(op.split("=", 1)[1], "cap_aws06_dashboard_ec2.png")
    elif op == "dashboard":
        dashboard()
    elif op == "plan":
        dashboard(url="http://localhost:8501/?vista=plan", nombre="cap14b_plan_accion.png")
    elif op == "budget":
        navegador(BUDGETS, "cap_aws02_budget.png", contiene=(PRESUPUESTO, "Budget", "Presupuesto", "Billing", "Facturaci"),
                  pintar=8)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
