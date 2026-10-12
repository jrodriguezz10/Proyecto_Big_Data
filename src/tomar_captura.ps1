# Capturas de pantalla REALES para el informe (Parte B).
#
# Uso (desde la carpeta del proyecto):
#   powershell -ExecutionPolicy Bypass -File src\tomar_captura.ps1            # todas
#   powershell -ExecutionPolicy Bypass -File src\tomar_captura.ps1 -Solo cap05,cap06
#
# Para cada captura de consola:
#  a) abre una ventana NUEVA de PowerShell maximizada, fondo oscuro y fuente Consolas legible
#     (se lanza con conhost.exe porque en Windows 11 "powershell" puede abrirse dentro de
#     Windows Terminal, donde no se controla la fuente ni el tamano de la ventana);
#  b) muestra "PS <carpeta>> <comando>" y EJECUTA el comando de verdad;
#  c) espera a que termine (+2 s), pone la ventana al frente y captura la pantalla con
#     System.Windows.Forms + System.Drawing (Graphics.CopyFromScreen) -> capturas\capNN_nombre.png;
#  d) si la salida no cabe, filtra INFO/WARN con Select-String, muestra el rango relevante
#     y/o divide en capNN_nombre.png, capNNb_nombre.png, capNNc_nombre.png ...
# Las capturas de Spark UI abren Microsoft Edge (perfil temporal) mientras un job mantiene viva
# la UI con SPARK_UI_HOLD.
param([string[]]$Solo = @(), [int]$Fuente = 18)

$ErrorActionPreference = 'Stop'
$Solo = @($Solo | ForEach-Object { $_ -split ',' } | Where-Object { $_ })
$Proyecto = Split-Path -Parent $PSScriptRoot
$Capturas = Join-Path $Proyecto 'capturas'
$Evid = Join-Path $Proyecto 'evidencias'
$Tmp = Join-Path $env:TEMP 'sedalib_capturas'
New-Item -ItemType Directory -Force -Path $Tmp | Out-Null

Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public class W32 {
    [DllImport("user32.dll")] public static extern bool SetProcessDpiAwarenessContext(IntPtr v);
    [DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
    [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern IntPtr FindWindow(string c, string t);
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
    [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h, int n);
    [DllImport("user32.dll")] public static extern bool BringWindowToTop(IntPtr h);
    [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
    [DllImport("user32.dll")] public static extern void keybd_event(byte k, byte s, uint f, UIntPtr e);
    [DllImport("user32.dll")] public static extern bool IsWindow(IntPtr h);
}
"@
# DPI por monitor: la captura usa pixeles fisicos (pantalla al 125 %)
if (-not [W32]::SetProcessDpiAwarenessContext([IntPtr](-4))) { [void][W32]::SetProcessDPIAware() }
Add-Type -AssemblyName System.Windows.Forms, System.Drawing

function Al-Frente([IntPtr]$h) {
    for ($i = 0; $i -lt 5; $i++) {
        [W32]::keybd_event(0x12, 0, 0, [UIntPtr]::Zero)      # ALT: permite SetForegroundWindow
        [void][W32]::ShowWindow($h, 3)                       # SW_MAXIMIZE
        [void][W32]::BringWindowToTop($h)
        [void][W32]::SetForegroundWindow($h)
        [W32]::keybd_event(0x12, 0, 2, [UIntPtr]::Zero)
        Start-Sleep -Milliseconds 300
        if ([W32]::GetForegroundWindow() -eq $h) { return }
    }
    Write-Warning "No se pudo confirmar la ventana al frente"
}

function Guardar-Pantalla([IntPtr]$h, [string]$archivo) {
    $b = [System.Windows.Forms.Screen]::FromHandle($h).Bounds
    $bmp = New-Object System.Drawing.Bitmap($b.Width, $b.Height)
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.CopyFromScreen($b.Location, [System.Drawing.Point]::Empty, $b.Size)
    $bmp.Save($archivo, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bmp.Dispose()
    Write-Host ("   captura -> " + $archivo + "  (" + $b.Width + "x" + $b.Height + ")")
}

function Captura-Consola {
    param([string]$Nombre, [string]$Comando, [string]$Log = '', [string]$Desde = '', [string]$Hasta = '',
          [int]$TimeoutMin = 30)
    Write-Host ("== " + $Nombre + " : " + $Comando)
    Get-ChildItem $Tmp -Filter ($Nombre + '.*') -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem $Capturas -Filter ($Nombre.Substring(0, 5) + '*' + $Nombre.Substring(5) + '.png') -ErrorAction SilentlyContinue |
        Remove-Item -Force
    $titulo = 'CAPTURA ' + $Nombre
    $cfg = Join-Path $Tmp ($Nombre + '.json')
    @{ nombre = $Nombre; titulo = $titulo; cwd = $Proyecto; mostrar = $Comando; comando = $Comando;
       log = $(if ($Log) { Join-Path $Evid $Log } else { '' }); desde = $Desde; hasta = $Hasta;
       tmp = $Tmp; fuente = $Fuente } | ConvertTo-Json | Set-Content -Path $cfg -Encoding UTF8
    $hijo = Join-Path $PSScriptRoot '_ventana.ps1'
    $proc = Start-Process conhost.exe -WindowStyle Maximized -PassThru -ArgumentList @(
        'powershell.exe', '-NoExit', '-NoLogo', '-ExecutionPolicy', 'Bypass', '-Command',
        ("& '" + $hijo + "' -Config '" + $cfg + "'"))
    # localizar la ventana por su titulo y maximizarla
    $h = [IntPtr]::Zero
    for ($i = 0; $i -lt 100 -and $h -eq [IntPtr]::Zero; $i++) {
        if (Test-Path (Join-Path $Tmp ($Nombre + '.ventana'))) {
            $w = Get-Process powershell -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowTitle -eq $titulo } |
                Select-Object -First 1
            if ($w) { $h = $w.MainWindowHandle; $pidHijo = $w.Id }
        }
        Start-Sleep -Milliseconds 200
    }
    if ($h -eq [IntPtr]::Zero) { throw "No se encontro la ventana $titulo" }
    Al-Frente $h
    Set-Content -Path (Join-Path $Tmp ($Nombre + '.max')) -Value 'ok'
    $limite = (Get-Date).AddMinutes($TimeoutMin)
    $p = 1
    while ($true) {
        $marca = Join-Path $Tmp ($Nombre + '.listo.' + $p)
        while (-not (Test-Path $marca)) {
            if ((Get-Date) -gt $limite) { throw "Timeout esperando $Nombre" }
            Start-Sleep -Milliseconds 500
        }
        $total = [int]((Get-Content $marca).Split('/')[1])
        Start-Sleep -Seconds 2
        Al-Frente $h
        Start-Sleep -Milliseconds 500
        # parte 1 = capNN_nombre.png; siguientes = capNNb_nombre.png, capNNc_nombre.png ...
        $archivo = if ($p -gt 1) { $Nombre.Substring(0, 5) + [char](96 + $p) + $Nombre.Substring(5) } else { $Nombre }
        Guardar-Pantalla $h (Join-Path $Capturas ($archivo + '.png'))
        Set-Content -Path (Join-Path $Tmp ($Nombre + '.ok.' + $p)) -Value 'ok'
        if ($p -ge $total) { break }
        $p++
    }
    Start-Sleep -Milliseconds 500
    Stop-Process -Id $pidHijo -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
}

function Esperar-UI([string]$hasta, [int]$TimeoutMin = 20) {
    # espera a que la app Spark viva en :4040 tenga un job SQL cuya descripcion coincida
    $limite = (Get-Date).AddMinutes($TimeoutMin)
    while ((Get-Date) -lt $limite) {
        try {
            $app = (Invoke-RestMethod 'http://localhost:4040/api/v1/applications' -TimeoutSec 5)[0].id
            $sql = Invoke-RestMethod ("http://localhost:4040/api/v1/applications/$app/sql?details=false&length=10000") -TimeoutSec 10
            $ok = @($sql | Where-Object { $_.description -match $hasta -and $_.status -eq 'COMPLETED' })
            if ($ok.Count -gt 0) { return @{ app = $app; sql = $sql } }
        } catch {}
        Start-Sleep -Seconds 3
    }
    throw "Timeout esperando la Spark UI ($hasta)"
}

function Captura-Navegador([string]$Nombre, [string]$Url, [double]$Escala = 1.0, [int]$Espera = 6, [string]$Buscar = '') {
    Write-Host ("== " + $Nombre + " : " + $Url)
    $perfil = Join-Path $Tmp 'edge_perfil'
    $edge = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
    $antes = @(Get-Process msedge -ErrorAction SilentlyContinue | ForEach-Object Id)
    # InPrivate + perfil temporal: sin cuenta, sin sincronizacion, sin traductor ni burbuja de restaurar
    Start-Process $edge -ArgumentList @("--user-data-dir=`"$perfil`"", '--inprivate', '--no-first-run',
        '--no-default-browser-check', '--disable-sync', '--hide-crash-restore-bubble', '--lang=en-US',
        '--disable-features=Translate,TranslateUI,msEdgeSidebarV2,EdgeCollections', '--new-window',
        '--start-maximized', "--force-device-scale-factor=$Escala", $Url) | Out-Null
    $h = [IntPtr]::Zero
    for ($i = 0; $i -lt 60 -and $h -eq [IntPtr]::Zero; $i++) {
        Start-Sleep -Milliseconds 500
        $w = Get-Process msedge -ErrorAction SilentlyContinue |
            Where-Object { $antes -notcontains $_.Id -and $_.MainWindowHandle -ne 0 -and $_.MainWindowTitle -match 'SEDALIB' } |
            Select-Object -First 1
        if ($w) { $h = $w.MainWindowHandle }
    }
    if ($h -eq [IntPtr]::Zero) { throw "No se encontro la ventana de Edge para $Url" }
    Al-Frente $h
    Start-Sleep -Seconds $Espera
    Al-Frente $h
    if ($Buscar) {
        # Ctrl+F lleva la vista al nodo buscado del DAG
        [System.Windows.Forms.SendKeys]::SendWait('^f')
        Start-Sleep -Milliseconds 600
        [System.Windows.Forms.SendKeys]::SendWait($Buscar)
        Start-Sleep -Milliseconds 400
        [System.Windows.Forms.SendKeys]::SendWait('{ENTER}')
        Start-Sleep -Milliseconds 600
        [System.Windows.Forms.SendKeys]::SendWait('{ESC}')
        Start-Sleep -Seconds 1
    }
    Guardar-Pantalla $h (Join-Path $Capturas ($Nombre + '.png'))
    Get-Process msedge -ErrorAction SilentlyContinue | Where-Object { $antes -notcontains $_.Id } |
        Stop-Process -Force -ErrorAction SilentlyContinue
}

function Job-ConUI([string]$Script, [string]$Log, [hashtable]$EnvVars) {
    Remove-Item (Join-Path $Evid '.liberar_ui') -Force -ErrorAction SilentlyContinue
    $e = ($EnvVars.GetEnumerator() | ForEach-Object { "-e $($_.Key)=$($_.Value)" }) -join ' '
    $ps1 = Join-Path $Tmp ('job_' + $Log + '.ps1')
    "docker exec $e spark_node sh -c '/spark/bin/spark-submit /app/src/$Script > /app/evidencias/$Log 2>&1'" |
        Set-Content -Path $ps1 -Encoding ASCII
    return Start-Process powershell.exe -WindowStyle Hidden -PassThru -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ps1)
}

function Liberar-UI($proc) {
    Set-Content -Path (Join-Path $Evid '.liberar_ui') -Value 'ok'
    $proc.WaitForExit(120000) | Out-Null
    Remove-Item (Join-Path $Evid '.liberar_ui') -Force -ErrorAction SilentlyContinue
}

function Quiere([string]$n) { return ($Solo.Count -eq 0) -or ($Solo -contains $n) }

$SPARK = 'docker exec spark_node /spark/bin/spark-submit'
$FILTRO = '2>&1 | Select-String -NotMatch " INFO | WARN |log4j|^\s+at "'

if (Quiere 'cap01') { Captura-Consola 'cap01_docker_ps' 'docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"' }
if (Quiere 'cap02') { Captura-Consola 'cap02_hdfs_raw' 'docker exec namenode hdfs dfs -ls -R /datalake_sedalib/raw' }
if (Quiere 'cap03') { Captura-Consola 'cap03_diagnostico' "$SPARK /app/src/diagnostico.py $FILTRO" -Log '00_diagnostico.txt' -Desde '0\.4 %' -Hasta '0\.6 VALORES' }
if (Quiere 'cap04') { Captura-Consola 'cap04_procesamiento' "$SPARK /app/src/procesamiento_spark.py $FILTRO" -Log '02_procesamiento.txt' }
if (Quiere 'cap05') { Captura-Consola 'cap05_calidad' "$SPARK /app/src/calidad_spark.py $FILTRO" -Log '03_calidad.txt' }
if (Quiere 'cap06') { Captura-Consola 'cap06_hdfs_processed' 'docker exec namenode hdfs dfs -ls -R /datalake_sedalib/processed' }
if (Quiere 'cap07') { Captura-Consola 'cap07_optimizacion' "$SPARK /app/src/optimizacion_spark.py $FILTRO" -Log '04_optimizacion.txt' }
if (Quiere 'cap08') {
    $job = Job-ConUI 'optimizacion_spark.py' '04b_optimizacion_ui.txt' @{ SPARK_UI_HOLD = 600 }
    $ui = Esperar-UI 'D_Broadcast_16part corrida 3'
    $a = ($ui.sql | Where-Object { $_.description -eq 'A_SortMerge_200part corrida 3' } | Select-Object -First 1).id
    $b = ($ui.sql | Where-Object { $_.description -eq 'B_Broadcast_200part corrida 3' } | Select-Object -First 1).id
    Captura-Navegador 'cap08_sparkui_sql_sortmerge' "http://localhost:4040/SQL/execution/?id=$a" 0.9 -Buscar 'SortMergeJoin'
    Captura-Navegador 'cap08b_sparkui_sql_broadcast' "http://localhost:4040/SQL/execution/?id=$b" 0.9 -Buscar 'BroadcastHashJoin'
    Liberar-UI $job
}
if (Quiere 'cap09') { Captura-Consola 'cap09_escalabilidad' "$SPARK /app/src/escalabilidad_spark.py $FILTRO" -Log '05_escalabilidad.txt' -Desde 'PRUEBA DE ESCALABILIDAD' -TimeoutMin 40 }
if (Quiere 'cap10') {
    $job = Job-ConUI 'escalabilidad_spark.py' '05c_escalabilidad_ui.txt' @{ SPARK_UI_HOLD = 600; ESCALAS = '1,10,100' }
    $ui = Esperar-UI 'Escalabilidad 100X'
    Start-Sleep -Seconds 8
    $stages = Invoke-RestMethod "http://localhost:4040/api/v1/applications/$($ui.app)/stages"
    $top = $stages | Sort-Object { [double]$_.diskBytesSpilled } -Descending | Select-Object -First 1
    Write-Host ("   stage con mas spill: " + $top.stageId + " disk=" + $top.diskBytesSpilled + " mem=" + $top.memoryBytesSpilled)
    Captura-Navegador 'cap10_sparkui_stages' ("http://localhost:4040/stages/stage/?id=" + $top.stageId + "&attempt=0") 0.8
    Captura-Navegador 'cap10b_sparkui_stages_lista' 'http://localhost:4040/stages/' 0.8
    Liberar-UI $job
}
if (Quiere 'cap11') { Captura-Consola 'cap11_eda' 'python src/graficos_locales.py' -Log '07_graficos_locales.txt' }
if (Quiere 'cap12') { Captura-Consola 'cap12_modelo' "$SPARK /app/src/analisis_modelo.py $FILTRO" -Log '06_analisis_modelo.txt' -Desde '4\.2 MODELO BASE' }
if (Quiere 'cap13') { Captura-Consola 'cap13_linea_base' "$SPARK /app/src/linea_base.py $FILTRO" -Log '01_linea_base.txt' }
Write-Host "Listo."
