"""7. Diagrama de arquitectura v2 -> graficos/arquitectura_v2.png (python src/arquitectura_v2.py, en el host)."""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

TINTA, TINTA2 = "#0b0b0b", "#52514e"
AZUL, AZUL_F = "#2a78d6", "#e8f1fc"
GRIS_F, NARANJA = "#f3f2ef", "#eb6834"

fig, ax = plt.subplots(figsize=(17, 9.5))
ax.set_xlim(0, 170)
ax.set_ylim(0, 95)
ax.axis("off")


def caja(x, y, w, h, titulo, cuerpo, fondo=AZUL_F, borde=AZUL, estilo="solid"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.5", fc=fondo, ec=borde,
                                lw=1.8, ls=estilo))
    ax.text(x + w / 2, y + h - 3, titulo, ha="center", va="top", fontsize=12, fontweight="bold", color=TINTA)
    ax.text(x + w / 2, y + h - 8.5, cuerpo, ha="center", va="top", fontsize=9, color=TINTA2, linespacing=1.45)


def flecha(x1, y1, x2, y2, contrato, dx=0, dy=2.2, estilo="solid", color=TINTA2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=16, lw=1.6, color=color, ls=estilo))
    ax.text((x1 + x2) / 2 + dx, (y1 + y2) / 2 + dy, contrato, ha="center", va="bottom", fontsize=8.2, color=TINTA,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#c9c8c3", lw=0.8))


ax.text(2, 92, "Arquitectura v2 - Datalake SEDALIB (Big Data, UPAO)", fontsize=16, fontweight="bold", color=TINTA)
ax.text(2, 88, "Flujo batch. Las etiquetas sobre las flechas son el contrato entre capas.", fontsize=10, color=TINTA2)

# Fila superior
caja(2, 50, 30, 33, "1. Fuentes", "SUNASS: interrupciones, ANF,\nhidrojets (CSV)\nSENAMHI: clima horario (CSV)\n"
     "INGEMMET: peligros (CSV ';')\nDATASS: antigüedad y estado\nde sistemas (XLSX)", fondo=GRIS_F, borde=TINTA2)
caja(46, 50, 30, 33, "2. Ingesta batch", "hdfs dfs -put (namenode)\n\ningesta_infraestructura.py:\nXLSX -> CSV ';' UTF-8\n"
     "(solo quita cabecera\ndecorativa del reporte)")
caja(90, 50, 34, 33, "3. HDFS  raw/", "hdfs://namenode:8020/\ndatalake_sedalib/raw/\n  clima/  geologia/\n"
     "  infraestructura/  operaciones/\n\nCSV inmutable (fuente de verdad)")
caja(136, 50, 32, 33, "4. Spark 3.2.1", "Modo LOCAL[16] en spark_node\n(producción: YARN)\n\nesquemas explícitos, limpieza,\n"
     "imputación, normalización,\nagregados + broadcast, rezagos")

# Fila inferior
caja(136, 6, 32, 33, "5. HDFS  processed/", "master_panel.parquet\n  ANIO=2019 ... ANIO=2024\n"
     "clima_distrito_mes.parquet\n\nParquet + Snappy,\npartitionBy(\"ANIO\")")
caja(90, 6, 34, 33, "6. Spark MLlib", "Regresión Logística\nÁrbol de Decisión\n\nsplit temporal: 2024 = prueba\n"
     "AUC-ROC, AUC-PR, matriz conf.\nvs. línea base ingenua")
caja(46, 6, 30, 33, "7. Power BI", "PLANIFICADO\n(no implementado)\n\ntablero de riesgo por\ndistrito-mes para SEDALIB",
     fondo="white", borde=NARANJA, estilo="--")
caja(2, 6, 30, 33, "Evidencias", "/app/evidencias/*.txt\n(salida de cada script)\n\ng1, g2, g3 (matplotlib,\n"
     "host) desde eda_g*.csv", fondo=GRIS_F, borde=TINTA2)

flecha(32.8, 66.5, 45.2, 66.5, "archivos tal cual\nse descargan", dy=3)
flecha(76.8, 66.5, 89.2, 66.5, "CSV sin modificar;\nXLSX -> CSV ';'", dy=3)
flecha(124.8, 66.5, 135.2, 66.5, "StructType\nexplícito\n(comun.py)", dy=3)
flecha(152, 49.2, 152, 39.8, "1 fila por (DEPTO_N, DIST_N,\nANIO, MES); 19 380 filas,\n27 col.; sin explosión de JOIN",
       dx=-21, dy=-4)
flecha(135.2, 22.5, 124.8, 22.5, "solo rezagos +\nestáticas (sin fuga)", dy=3)
flecha(89.2, 22.5, 76.8, 22.5, "scores por distrito-mes\n(Parquet/CSV)", dy=3, estilo="--", color=NARANJA)

ax.text(2, 1, "Contenedores Docker: namenode + datanode (HDFS 3.2.1), spark_node (Spark 3.2.1, Python 3.7). "
         "Línea punteada naranja = componente planificado.", fontsize=9, color=TINTA2)
fig.savefig(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "graficos", "arquitectura_v2.png"),
            dpi=150, bbox_inches="tight")
print("graficos/arquitectura_v2.png generado")
