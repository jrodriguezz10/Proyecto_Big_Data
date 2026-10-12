# Ventana de consola usada por tomar_captura.ps1 (no se ejecuta sola).
# Muestra el prompt y el comando como lo escribiria una persona, lo EJECUTA de verdad,
# guarda la salida completa en evidencias\ y la muestra por paginas (_a, _b, ...) si no cabe.
param([string]$Config)

$c = Get-Content -Raw -Path $Config | ConvertFrom-Json

Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public class ConFont {
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    public struct CONSOLE_FONT_INFOEX {
        public uint cbSize; public uint nFont; public short X; public short Y;
        public int FontFamily; public int FontWeight;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)] public string FaceName;
    }
    [DllImport("kernel32.dll", SetLastError = true)] public static extern IntPtr GetStdHandle(int h);
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool SetCurrentConsoleFontEx(IntPtr h, bool max, ref CONSOLE_FONT_INFOEX f);
    public static bool Set(string face, short size) {
        CONSOLE_FONT_INFOEX f = new CONSOLE_FONT_INFOEX();
        f.cbSize = (uint)Marshal.SizeOf(f); f.FaceName = face; f.Y = size; f.X = 0;
        f.FontFamily = 54; f.FontWeight = 400;
        return SetCurrentConsoleFontEx(GetStdHandle(-11), false, ref f);
    }
}
"@

# Fondo oscuro, fuente legible, ventana al maximo de la pantalla
[void][ConFont]::Set("Consolas", [int16]$c.fuente)
$ui = $Host.UI.RawUI
$ui.WindowTitle = $c.titulo
$ui.BackgroundColor = 'Black'
$ui.ForegroundColor = 'Gray'
Start-Sleep -Milliseconds 400
$max = $ui.MaxPhysicalWindowSize
# con la fuente nueva caben menos columnas: primero la ventana, luego el bufer al mismo ancho
try { $ui.WindowSize = New-Object Management.Automation.Host.Size($max.Width, $max.Height) } catch {}
try { $ui.BufferSize = New-Object Management.Automation.Host.Size($max.Width, 9999) } catch {}
[Console]::OutputEncoding = [Text.Encoding]::UTF8
Set-Content -Path (Join-Path $c.tmp ($c.nombre + '.ventana')) -Value 'ok'
# espera a que la ventana este maximizada antes de medir filas/columnas
while (-not (Test-Path (Join-Path $c.tmp ($c.nombre + '.max')))) { Start-Sleep -Milliseconds 200 }
Start-Sleep -Milliseconds 500
# bufer = ancho visible de la ventana maximizada, para que las lineas largas envuelvan y no se corten
$max = $ui.MaxPhysicalWindowSize
# 2 columnas menos: la barra de desplazamiento tapa la ultima columna visible
try { $ui.WindowSize = New-Object Management.Automation.Host.Size(([Math]::Min($ui.WindowSize.Width, $max.Width) - 2), [Math]::Min($ui.WindowSize.Height, $max.Height)) } catch {}
try { $ui.BufferSize = New-Object Management.Automation.Host.Size($ui.WindowSize.Width, 9999) } catch {}
Clear-Host

function Mostrar-Prompt([string]$extra) {
    Write-Host ("PS " + $c.cwd + "> ") -NoNewline -ForegroundColor Gray
    Write-Host $c.mostrar -ForegroundColor White -NoNewline
    if ($extra) { Write-Host ("   " + $extra) -ForegroundColor DarkGray } else { Write-Host "" }
}

Set-Location $c.cwd
Mostrar-Prompt ""
$ErrorActionPreference = 'Continue'
$lineas = @(Invoke-Expression $c.comando | ForEach-Object { $_.ToString().TrimEnd() })

# evidencia completa de ESTA ejecucion
if ($c.log) { $lineas | Out-File -FilePath $c.log -Encoding utf8 }

# rango relevante (Desde/Hasta) para que se lea el resultado
$ini = 0; $fin = $lineas.Count
if ($c.desde) { for ($i = 0; $i -lt $lineas.Count; $i++) { if ($lineas[$i] -match $c.desde) { $ini = [Math]::Max(0, $i - 1); break } } }
if ($c.hasta) { for ($i = $ini + 2; $i -lt $lineas.Count; $i++) { if ($lineas[$i] -match $c.hasta) { $fin = [Math]::Max($ini, $i - 1); break } } }
$vista = if ($fin -gt $ini) { $lineas[$ini..($fin - 1)] } else { @() }

# paginacion segun filas visibles (contando lineas que envuelven)
$ancho = $ui.WindowSize.Width
# filas libres: alto - filas del prompt (puede envolver) - cabecera - cursor
$filasPrompt = [Math]::Ceiling(("PS " + $c.cwd + "> " + $c.mostrar + "   # misma ejecucion, continuacion de la salida (parte b)").Length / [double]$ancho)
$alto = $ui.WindowSize.Height - $filasPrompt - 2
$paginas = @(); $actual = @(); $usadas = 0
foreach ($l in $vista) {
    $filas = [Math]::Max(1, [Math]::Ceiling($l.Length / [double]$ancho))
    if ($usadas + $filas -gt $alto -and $actual.Count -gt 0) { $paginas += , $actual; $actual = @(); $usadas = 0 }
    $actual += $l; $usadas += $filas
}
$paginas += , $actual
$n = $paginas.Count

for ($p = 0; $p -lt $n; $p++) {
    if ($p -gt 0) {
        Clear-Host
        Mostrar-Prompt ("# misma ejecucion, continuacion de la salida (parte " + [char](97 + $p) + ")")
    }
    elseif ($ini -gt 0 -or $fin -lt $lineas.Count) {
        Write-Host ("# salida filtrada: lineas " + ($ini + 1) + "-" + $fin + " de " + $lineas.Count + " (completa en " + (Split-Path $c.log -Leaf) + ")") -ForegroundColor DarkGray
    }
    foreach ($l in $paginas[$p]) { Write-Host $l }
    Set-Content -Path (Join-Path $c.tmp ($c.nombre + '.listo.' + ($p + 1))) -Value ("" + ($p + 1) + "/" + $n)
    while (-not (Test-Path (Join-Path $c.tmp ($c.nombre + '.ok.' + ($p + 1))))) { Start-Sleep -Milliseconds 300 }
}
