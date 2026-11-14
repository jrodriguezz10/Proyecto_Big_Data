"""7. Diagrama de arquitectura v2 (ruta hibrida local + AWS) -> graficos/arquitectura_v2.png.

python src/arquitectura_v2.py   (en el host)
Carril superior: desarrollo, ingesta y escalabilidad en local (Docker: HDFS + Spark; YARN en produccion).
Carril inferior: replica en S3 (raw/processed/curated) y el MISMO codigo de src/ en EMR Serverless.
No muestra el ID de la cuenta AWS (el bucket se escribe como s3://<bucket>/).
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402

TINTA, TINTA2 = "#0b0b0b", "#52514e"
AZUL, AZUL_F = "#2a78d6", "#e8f1fc"
GRIS_F, NARANJA = "#f3f2ef", "#eb6834"
VERDE, VERDE_F = "#1f8a5b", "#e6f4ec"
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

fig, ax = plt.subplots(figsize=(19, 11.5))
ax.set_xlim(0, 178)
ax.set_ylim(0, 112)
ax.axis("off")


def caja(x, y, w, h, titulo, cuerpo, fondo=AZUL_F, borde=AZUL, estilo="solid"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.5", fc=fondo, ec=borde,
                                lw=1.8, ls=estilo))
    ax.text(x + w / 2, y + h - 2.5, titulo, ha="center", va="top", fontsize=11.5, fontweight="bold", color=TINTA)
    ax.text(x + w / 2, y + h - 7.5, cuerpo, ha="center", va="top", fontsize=8.6, color=TINTA2, linespacing=1.4)


def flecha(x1, y1, x2, y2, contrato, dx=0, dy=2.0, estilo="solid", color=TINTA2, ha="center"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=16, lw=1.6, color=color,
                                 ls=estilo))
    if contrato:
        ax.text((x1 + x2) / 2 + dx, (y1 + y2) / 2 + dy, contrato, ha=ha, va="bottom", fontsize=7.9, color=TINTA,
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#c9c8c3", lw=0.8))


def carril(y, h, etiqueta, color):
    ax.add_patch(Rectangle((0.5, y), 177, h, fc=color, ec="none", alpha=0.35, zorder=0))
    ax.text(1.5, y + h - 1.5, etiqueta, fontsize=10.5, fontweight="bold", color=TINTA, va="top")


ax.text(1, 109.5, "Arquitectura v2 - Datalake SEDALIB: ruta híbrida local (HDFS/YARN) + AWS (S3 + EMR Serverless)",
        fontsize=16, fontweight="bold", color=TINTA)
ax.text(1, 105.5, "Batch. Las etiquetas sobre las flechas son el contrato entre capas. El mismo código de src/ corre en "
        "ambos carriles; solo cambian variables de entorno (comun.py).", fontsize=10, color=TINTA2)

W, H = 25, 31
X = [3, 32.5, 62, 91.5, 121, 150.5]

# ------------------------------------------------------------------ carril local
carril(59, 44, "LOCAL (Docker): desarrollo, ingesta y escalabilidad", GRIS_F)
YL = 62
caja(X[0], YL, W, H, "1. Fuentes", "SUNASS: interrupciones,\nANF, hidrojets (CSV)\nSENAMHI: clima (CSV)\n"
     "NASA POWER: clima\nLa Libertad (CSV)\nINGEMMET: peligros (CSV)\nDATASS: infraestructura\n(XLSX)",
     fondo="white", borde=TINTA2)
caja(X[1], YL, W, H, "2. Ingesta batch", "hdfs dfs -put (namenode)\n\ningesta_infraestructura.py:\nXLSX -> CSV ';' UTF-8\n"
     "ingesta_clima_nasa.py:\nAPI NASA POWER -> CSV")
caja(X[2], YL, W, H, "3. HDFS raw/", "hdfs://namenode:8020/\ndatalake_sedalib/raw/\nclima/ geologia/\n"
     "infraestructura/\noperaciones/\n\nCSV inmutable")
caja(X[3], YL, W, H, "4. Spark 3.2.1", "local[*] en spark_node\n(producción: YARN)\n\nesquemas explícitos,\nlimpieza, JOINs,\n"
     "rezagos, anomalía clima\n\nescalabilidad: Spark vs\npandas (OOM, spill)")
caja(X[4], YL, W, H, "5. HDFS processed/", "master_panel\nmaster_panel_clima\nclima_distrito_mes\n(.parquet)\n\n"
     "Parquet + Snappy,\npartitionBy(\"ANIO\")")
caja(X[5], YL, W, H, "6. MLlib + evidencias", "rolling-origin (4 cortes)\nholdout 2024-01..05\n"
     "M1-GBT AUC-PR 0.7293\n\nevidencias/*.txt\ngraficos/ g1..g10")

yl = YL + H / 2
flecha(X[0] + W + 0.8, yl, X[1] - 0.8, yl, "tal cual se\ndescargan")
flecha(X[1] + W + 0.8, yl, X[2] - 0.8, yl, "CSV sin\nmodificar")
flecha(X[2] + W + 0.8, yl, X[3] - 0.8, yl, "StructType\n(comun.py)")
flecha(X[3] + W + 0.8, yl, X[4] - 0.8, yl, "19 380 filas\ndistrito-mes")
flecha(X[4] + W + 0.8, yl, X[5] - 0.8, yl, "rezagos +\nestáticas")

# ------------------------------------------------------------------ carril AWS
carril(2, 54, "AWS us-east-2: réplica y ejecución gestionada", VERDE_F)
YA = 6
caja(X[0], YA, W, 38, "Gobierno y costo", "IAM: rol de ejecución con\nmínimo privilegio\n(raw/ lectura; processed/,\n"
     "curated/, logs/ escritura)\n\nbucket: bloqueo público,\nSSE-S3, versionado,\netiquetas\n\nAWS Budgets: 10 USD/mes\n"
     "(avisos 50 % y 80 %)", fondo="white", borde=VERDE)
caja(X[1], YA, W, 38, "Prueba de sustitución", "mismo src/ sin cambiar\nla lógica:\nSEDALIB_DATALAKE=s3://\n"
     "SEDALIB_SPARK_MASTER=auto\n\nsilver: 19 380 filas\n(= local)\nM1-GBT AUC-PR\n0.7293 local / 0.7248 AWS\n\n"
     "4 jobs: 11.8 min, 0.11 USD", fondo="white", borde=VERDE)
caja(X[2], YA, W, H, "S3 raw/", "s3://<bucket>/raw/\nclima/ geologia/\ninfraestructura/\noperaciones/\n\n"
     "10 archivos, 92 MB\n(mismos CSV)", fondo=VERDE_F, borde=VERDE)
caja(X[3], YA, W, H, "EMR Serverless", "sedalib-spark\nemr-7.9.0 (Spark 3.5.5)\n\ndriver + <= 3 executors\n"
     "2 vCPU / 8 GB c/u\nautoapagado 5 min\n\ncódigo: s3://<bucket>/\nscripts/", fondo=VERDE_F, borde=VERDE)
caja(X[4], YA, W, H, "S3 processed/", "master_panel\nmaster_panel_clima\nclima_distrito_mes\n(.parquet)\n\n"
     "jobs 1-3: procesamiento,\ncalidad, integración clima", fondo=VERDE_F, borde=VERDE)
caja(X[5], YA, W, H, "S3 curated/", "salidas del modelo:\nmetricas_modelos.csv\npred. holdout 2024\n\n"
     "logs/: stdout por job\n\nPower BI (planificado)", fondo=VERDE_F, borde=VERDE)

ya = YA + H / 2
flecha(X[2] + W + 0.8, ya, X[3] - 0.8, ya, "lee raw/\n(rol IAM)")
flecha(X[3] + W + 0.8, ya, X[4] - 0.8, ya, "jobs 1-3\nescriben")
flecha(X[4] + W + 0.8, ya, X[5] - 0.8, ya, "job 4: modelo\n(EMR) -> CSV")

# replica local -> S3 (de los archivos de raw/ locales)
flecha(X[2] + W / 2, YL - 0.8, X[2] + W / 2, YA + H + 0.8,
       "réplica: aws/01_setup_s3.py (boto3)\nomite archivos con el mismo tamaño", dx=1.5, dy=-2, ha="left",
       color=VERDE)
# mismo codigo
flecha(X[3] + W / 2, YL - 0.8, X[3] + W / 2, YA + H + 0.8,
       "mismo código src/\n(aws/03_pipeline_emr.py)", dx=1.5, dy=-2, ha="left", color=VERDE, estilo="--")

ax.text(1, -1.2, "Local: contenedores Docker namenode + datanode (HDFS 3.2.1) y spark_node (Spark 3.2.1). "
        "AWS: S3 + EMR Serverless + IAM + Budgets, todo creado por código (aws/). "
        "Línea verde punteada = mismo código desplegado.", fontsize=8.8, color=TINTA2)
fig.savefig(os.path.join(BASE, "graficos", "arquitectura_v2.png"), dpi=150, bbox_inches="tight")
print("graficos/arquitectura_v2.png generado")
