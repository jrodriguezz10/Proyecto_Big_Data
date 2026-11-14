import csv, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
src, out = sys.argv[1], sys.argv[2]
AZUL, TINTA, TINTA2, GRILLA, NEUTRO = "#2a78d6", "#0b0b0b", "#52514e", "#e4e3df", "#8a8984"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10.5, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})
rows = [r for r in csv.DictReader(open(src)) if not r["tramo"].startswith("9")]
etq = {"1": "< 0.5\nmuy seco", "2": "0.5 – 1", "3": "1 – 1.5", "4": "1.5 – 3", "5": "> 3\nmuy lluvioso"}
x = [etq[r["tramo"][0]] for r in rows]
n = [int(r["n"]) for r in rows]; k = [int(r["k"]) for r in rows]
tasa = [100 * a / b for a, b in zip(k, n)]
glob = 100 * sum(k) / sum(n)
fig, ax = plt.subplots(figsize=(9, 5.4))
fig.subplots_adjust(top=0.78, bottom=0.22, left=0.08, right=0.97)
ax.bar(range(len(x)), tasa, width=0.6, color=AZUL, zorder=2)
for i, (t, a, b) in enumerate(zip(tasa, k, n)):
    ax.text(i, t + 0.6, "%.1f %%" % t, ha="center", va="bottom", fontsize=11, color=TINTA, fontweight="bold")
    ax.text(i, t + 2.6, "%d / %d" % (a, b), ha="center", va="bottom", fontsize=8.5, color=TINTA2)
ax.axhline(glob, color=NEUTRO, lw=1.2, ls="--", zorder=1)
ax.text(2, glob + 0.4, "promedio La Libertad %.1f %%" % glob, ha="center", va="bottom",
        fontsize=8.5, color=TINTA2)
ax.axvline(2.5, color=GRILLA, lw=1, zorder=0)
ax.text(2.55, 27.5, "MES_LLUVIOSO = 1 →", fontsize=8.5, color=TINTA2, ha="left")
ax.set_xticks(range(len(x))); ax.set_xticklabels(x)
ax.set_xlabel("Anomalía de precipitación del mes (PP del mes / promedio histórico del mismo mes y distrito)")
ax.set_ylabel("Distrito-meses con interrupción (%)")
ax.set_ylim(0, 30); ax.grid(axis="y", color=GRILLA, lw=0.8, zorder=0)
fig.suptitle("g2b. La Libertad: tasa de interrupción según anomalía de lluvia del mes", x=0.01, ha="left",
             fontsize=13, color=TINTA, fontweight="bold")
fig.text(0.01, 0.845, "13 distritos, 2019-06 a 2024-05 (780 distrito-meses). Descriptivo: lluvia del mismo mes, sin rezago.\n"
         "Etiqueta: tasa y distrito-meses con interrupción / total del tramo.", fontsize=9.5, color=TINTA2)
fig.text(0.01, 0.01, "Fuente: precipitación NASA POWER (centroide de distrito); interrupciones SUNASS.\n"
         "Cálculo: master_panel_clima.parquet (Spark), evidencias/02c_pregunta2_la_libertad.txt.",
         fontsize=8.5, color=TINTA2)
fig.savefig(out, dpi=150)
