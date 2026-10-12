"""6b. Graficos del EDA (se ejecuta en el host: python src/graficos_locales.py).

Lee las cifras REALES que escribe analisis_modelo.py en evidencias/eda_g*.csv
(no hay valores escritos a mano) y genera g1/g2/g3 en PNG. Imprime las cifras de cada grafico.
"""
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRAF = os.path.join(BASE, "graficos")
EVID = os.path.join(BASE, "evidencias")
AZUL, NARANJA = "#2a78d6", "#eb6834"
TINTA, TINTA2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})


def leer(nombre):
    with open(os.path.join(EVID, nombre), encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fuente(fig, texto):
    fig.text(0.01, 0.01, texto, fontsize=8.5, color=TINTA2, ha="left", va="bottom")


def etiquetas(ax, barras, fmt):
    for b in barras:
        ax.annotate(fmt(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 3), textcoords="offset points", ha="center", fontsize=9, color=TINTA)


# ---------------------------------------------------------------- g1
g1 = leer("eda_g1_distritos_ll.csv")
print("g1 - Interrupciones por distrito de La Libertad (2019-06 a 2024-05)")
for r in g1:
    print("   %-22s interrupciones=%4s imprevistas=%4s meses_con_int=%3s tasa=%s %%"
          % (r["DIST_N"], r["interrupciones"], r["imprevistas"], r["meses_con_int"], r["tasa_int_pct"]))
fig, ax = plt.subplots(figsize=(10, 6))
datos = list(reversed(g1))
bar = ax.barh([r["DIST_N"].title() for r in datos], [int(r["interrupciones"]) for r in datos], color=AZUL, height=0.65)
for b in bar:
    ax.annotate(str(int(b.get_width())), (b.get_width(), b.get_y() + b.get_height() / 2), xytext=(4, 0),
                textcoords="offset points", va="center", fontsize=9, color=TINTA)
ax.set_title("g1. Interrupciones del servicio por distrito - La Libertad (SEDALIB)", loc="left", fontsize=13, color=TINTA)
ax.set_xlabel("Interrupciones registradas (jun-2019 a may-2024)")
ax.set_ylabel("Distrito")
ax.xaxis.grid(True, color=GRILLA)
ax.set_axisbelow(True)
fuente(fig, "Fuente: SUNASS - registro de interrupciones (datos abiertos). Procesamiento: Spark, panel distrito-mes.")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(os.path.join(GRAF, "g1_distritos_la_libertad.png"), dpi=150)
plt.close(fig)

# ---------------------------------------------------------------- g2
g2 = leer("eda_g2_tasa_por_pp.csv")
print("\ng2 - Tasa de interrupcion por rango de PP_TOTAL mensual (LIMA, AREQUIPA, TACNA; sin La Libertad)")
for r in g2:
    print("   %-10s distrito-meses=%5s meses_con_int=%5s tasa=%6s %%"
          % (r["RANGO_PP"], r["distrito_meses"], r["meses_con_int"], r["tasa_int_pct"]))
fig, ax = plt.subplots(figsize=(10, 6))
bar = ax.bar([r["RANGO_PP"] for r in g2], [float(r["tasa_int_pct"]) for r in g2], color=AZUL, width=0.6)
etiquetas(ax, bar, lambda v: "%.1f %%" % v)
for b, r in zip(bar, g2):
    ax.annotate("n=%s" % r["distrito_meses"], (b.get_x() + b.get_width() / 2, 0), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=8.5, color="white")
ax.set_title("g2. Tasa de interrupción según precipitación mensual del departamento", loc="left", fontsize=13, color=TINTA)
ax.set_xlabel("PP_TOTAL mensual (mm, promedio de estaciones SENAMHI del departamento)")
ax.set_ylabel("Distrito-meses con al menos una interrupción (%)")
ax.yaxis.grid(True, color=GRILLA)
ax.set_axisbelow(True)
fuente(fig, "Fuente: SENAMHI (5 estaciones: Lima, Arequipa, Tacna) y SUNASS. La Libertad no tiene estación SENAMHI "
            "en los datos: ámbito nacional disponible.")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(os.path.join(GRAF, "g2_tasa_por_precipitacion.png"), dpi=150)
plt.close(fig)

# ---------------------------------------------------------------- g3
g3 = leer("eda_g3_tasa_por_peligros.csv")
print("\ng3 - Tasa de interrupcion por cantidad de peligros INGEMMET en el distrito")
for r in g3:
    print("   %-15s %-6s distritos=%4s distrito-meses=%5s tasa=%6s %%"
          % (r["AMBITO"], r["RANGO_PELIGROS"], r["distritos"], r["distrito_meses"], r["tasa_int_pct"]))
rangos = ["0", "1-5", "6-20", "21-50", ">50"]
fig, ax = plt.subplots(figsize=(10, 6))
ancho = 0.38
for i, (amb, color, nombre) in enumerate([("RESTO DEL PAIS", AZUL, "Resto del país"), ("LA LIBERTAD", NARANJA, "La Libertad")]):
    d = {r["RANGO_PELIGROS"]: r for r in g3 if r["AMBITO"] == amb}
    xs = [k + (i - 0.5) * ancho for k in range(len(rangos)) if rangos[k] in d]
    ys = [float(d[x]["tasa_int_pct"]) for x in rangos if x in d]
    bar = ax.bar(xs, ys, width=ancho - 0.03, color=color, label=nombre)
    for b, x in zip(bar, [x for x in rangos if x in d]):
        ax.annotate("%.1f %%\n(%s distr.)" % (b.get_height(), d[x]["distritos"]),
                    (b.get_x() + b.get_width() / 2, b.get_height()), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontsize=8, color=TINTA)
ax.set_xticks(range(len(rangos)))
ax.set_xticklabels(rangos)
ax.set_title("g3. Tasa de interrupción según peligros geológicos inventariados", loc="left", fontsize=13, color=TINTA)
ax.set_xlabel("Cantidad de peligros INGEMMET en el distrito")
ax.set_ylabel("Distrito-meses con al menos una interrupción (%)")
ax.yaxis.grid(True, color=GRILLA)
ax.set_axisbelow(True)
ax.legend(frameon=False, loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.0))
ax.set_ylim(0, max(float(r["tasa_int_pct"]) for r in g3) * 1.32)
fuente(fig, "Fuente: INGEMMET (inventario de peligros geológicos) y SUNASS. La Libertad = 13 distritos con "
            "interrupciones reportadas.")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(os.path.join(GRAF, "g3_tasa_por_peligros.png"), dpi=150)
plt.close(fig)
print("\nPNG generados: g1_distritos_la_libertad.png, g2_tasa_por_precipitacion.png, g3_tasa_por_peligros.png")
