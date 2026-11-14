"""g11. Modelo dimensional (estrella) de la capa gold -> graficos/g11_modelo_dimensional.png

python src/g11_modelo_dimensional.py   (en el host)
Si existe evidencias/aws_05_estado.json / dashboard/data/*.csv se anotan las filas reales de cada tabla.
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "dashboard", "data")
TINTA, TINTA2, SUPERFICIE, BORDE = "#0b0b0b", "#52514e", "#fcfcfb", "#c9c8c3"
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"


def filas(t):
    p = os.path.join(DATA, t + ".csv")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def caja(ax, x, y, w, titulo, cols, color, nota, clave=()):
    h = 0.62 + 0.36 * len(cols)
    ax.add_patch(FancyBboxPatch((x, y - h), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                facecolor="white", edgecolor=color, linewidth=2.2, zorder=2))
    ax.add_patch(FancyBboxPatch((x, y - 0.62), w, 0.62, boxstyle="round,pad=0.02,rounding_size=0.12",
                                facecolor=color, edgecolor=color, linewidth=2.2, zorder=3))
    ax.text(x + 0.18, y - 0.31, titulo, color="white", fontsize=11.5, fontweight="bold", va="center", zorder=4)
    for i, (c, tipo) in enumerate(cols):
        yy = y - 0.62 - 0.36 * (i + 0.5)
        ax.text(x + 0.2, yy, ("PK " if c in clave else "    ") + c, fontsize=9.6, va="center", color=TINTA,
                fontweight="bold" if c in clave else "normal", family="DejaVu Sans Mono", zorder=4)
        ax.text(x + w - 0.18, yy, tipo, fontsize=8.8, va="center", ha="right", color=TINTA2, zorder=4)
    ax.text(x + w / 2, y - h - 0.25, nota, ha="center", va="top", fontsize=9, color=TINTA2)
    return (x, y - h, w, h)


def n(t):
    k = filas(t)
    return "%s filas" % "{:,}".format(k).replace(",", " ") if k is not None else ""


fig, ax = plt.subplots(figsize=(15, 9.4))
fig.patch.set_facecolor(SUPERFICIE)
ax.set_facecolor(SUPERFICIE)
ax.set_xlim(0, 15)
ax.set_ylim(-0.9, 8.5)
ax.axis("off")

hechos = caja(ax, 5.4, 7.9, 4.2, "gold_riesgo_distrito_mes",
              [("ubigeo", "string"), ("departamento", "string"), ("distrito", "string"), ("anio", "int"),
               ("mes", "int"), ("prob_interrupcion", "double"), ("alerta", "int"), ("interrupcion_real", "int")],
              NARANJA, "TABLA DE HECHOS | %s\ngranularidad: 1 fila = 1 distrito x 1 mes" % n("gold_riesgo_distrito_mes"),
              clave=("departamento", "distrito", "anio", "mes"))
dd = caja(ax, 0.3, 7.9, 3.6, "dim_distrito",
          [("ubigeo", "string"), ("departamento", "string"), ("provincia", "string"), ("distrito", "string"),
           ("n_peligros", "int"), ("cobertura_clima", "double")],
          AZUL, "DIMENSION | %s\nubigeo: INEI UBIGEO 2022" % n("dim_distrito"), clave=("departamento", "distrito"))
dt = caja(ax, 11.1, 7.9, 3.6, "dim_tiempo",
          [("anio", "int"), ("mes", "int"), ("trimestre", "int"), ("temporada_lluvia", "int")],
          AZUL, "DIMENSION | %s\ntemporada_lluvia = 1 de diciembre a abril" % n("dim_tiempo"), clave=("anio", "mes"))
kp = caja(ax, 11.1, 3.15, 3.6, "gold_kpi_mensual",
          [("anio, mes", "int"), ("departamento", "string"), ("es_la_libertad", "int"), ("tasa_interrupcion", "double"),
           ("distritos_en_alerta", "bigint"), ("precision_alertas", "double"), ("recall_alertas", "double")],
          AQUA, "AGREGADO DERIVADO | %s\nmes x departamento (+ NACIONAL)" % n("gold_kpi_mensual"))
cm = caja(ax, 0.3, 3.15, 3.6, "gold_comparacion_modelos",
          [("ambito", "string"), ("modelo", "string"), ("auc_pr, auc_roc", "double"), ("umbral", "double"),
           ("precision, recall", "double")],
          AQUA, "AGREGADO DERIVADO | %s\nholdout 2024: M1-GBT vs tasa historica\nvs regla ingenua" % n("gold_comparacion_modelos"))


def flecha(p, q, texto, color=TINTA2, ls="-", dx=0.0, dy=0.15, ha="center", va="bottom"):
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=16, color=color, lw=1.8, ls=ls, zorder=1))
    ax.text((p[0] + q[0]) / 2 + dx, (p[1] + q[1]) / 2 + dy, texto, ha=ha, va=va, fontsize=8.8, color=color,
            linespacing=1.3)


hx, hy, hw, hh = hechos
flecha((hx, 6.6), (dd[0] + dd[2], 6.6), "N:1\ndepartamento,\ndistrito")
flecha((hx + hw, 6.6), (dt[0], 6.6), "N:1\nanio, mes")
flecha((hx + hw, hy + 0.5), (kp[0], kp[1] + kp[3] - 0.31), "GROUP BY\nanio, mes,\ndepartamento", ls="--",
       dx=0.15, dy=0.1, ha="left")
flecha((hx, hy + 0.5), (cm[0] + cm[2], cm[1] + cm[3] - 0.31), "holdout 2024\n(anio = 2024)", ls="--",
       dx=-0.15, dy=0.1, ha="right")

fig.suptitle("g11. Modelo dimensional de la capa gold (esquema estrella)", x=0.02, y=0.985, ha="left", fontsize=15,
             fontweight="bold", color=TINTA)
fig.text(0.02, 0.925, "s3://<bucket>/curated/gold/<tabla>/ (Parquet) registrado en Glue Data Catalog, base sedalib_gold "
         "(CREATE EXTERNAL TABLE desde Athena).\nHechos: probabilidad M1-GBT fuera de muestra 2022-01..2024-05, "
         "alerta = prob >= 0.1084.", fontsize=9.4, color=TINTA2, wrap=True)
fig.text(0.02, 0.02, "PK = clave del grano. Flechas solidas = relacion de la estrella; punteadas = tablas agregadas "
         "derivadas de los hechos. Sin datos personales (panel agregado distrito-mes).", fontsize=8.8, color=TINTA2)
fig.savefig(os.path.join(BASE, "graficos", "g11_modelo_dimensional.png"), dpi=150, facecolor=SUPERFICIE)
print("graficos/g11_modelo_dimensional.png generado")
