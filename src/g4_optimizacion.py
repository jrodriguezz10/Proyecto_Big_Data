"""g4. Mediana de tiempo por modo de JOIN (python src/g4_optimizacion.py, en el host).

Lee las cifras REALES del RESUMEN de evidencias/04_optimizacion.txt (no hay valores escritos a mano).
"""
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias", "04_optimizacion.txt")
SALIDA = os.path.join(BASE, "graficos", "g4_optimizacion.png")
AZUL, NARANJA = "#2a78d6", "#eb6834"
TINTA, TINTA2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})

# lineas del RESUMEN:  A_SortMerge_200part   0.663, 0.536, 0.461   0.536 s  {'SortMergeJoin': 3}
texto = open(EVID, encoding="utf-8").read()
resumen = texto[texto.index("RESUMEN"):]
patron = re.compile(r"^([A-D])_(\w+?)_(\d+)part\s+([\d.]+(?:, [\d.]+)+)\s+([\d.]+) s\s+\{'(\w+)'", re.M)
modos = [(m.group(1), m.group(2), int(m.group(3)), [float(x) for x in m.group(4).split(", ")],
          float(m.group(5)), m.group(6)) for m in patron.finditer(resumen)]
assert len(modos) == 4, "No se encontraron los 4 modos en %s" % EVID
base = modos[0][4]

print("g4 - Mediana por modo (fuente: evidencias/04_optimizacion.txt)")
for letra, tipo, part, corridas, med, join in modos:
    print("   %s %-10s %3d particiones | corridas=%s | mediana=%.3f s | speedup=%.2fx | %s"
          % (letra, tipo, part, corridas, med, base / med, join))

fig, ax = plt.subplots(figsize=(10, 6))
etiquetas = ["%s\n%s\n%d particiones" % (l, "SortMergeJoin" if t == "SortMerge" else "BroadcastHashJoin", p)
             for l, t, p, _, _, _ in modos]
colores = [AZUL if t == "SortMerge" else NARANJA for _, t, _, _, _, _ in modos]
barras = ax.bar(etiquetas, [m[4] for m in modos], color=colores, width=0.6)
for b, (letra, _, _, _, med, _) in zip(barras, modos):
    txt = "%.3f s" % med + ("\n(línea base)" if letra == "A" else "\nspeedup %.2f×" % (base / med))
    ax.annotate(txt, (b.get_x() + b.get_width() / 2, b.get_height()), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=9.5, color=TINTA)
ax.set_title("g4. Optimización de JOINs: mediana de 3 corridas por modo", loc="left", fontsize=13, color=TINTA)
ax.set_ylabel("Mediana del tiempo de la consulta (s)")
ax.set_xlabel("Modo (tipo de JOIN y spark.sql.shuffle.partitions)")
ax.set_ylim(0, base * 1.3)
ax.yaxis.grid(True, color=GRILLA)
ax.set_axisbelow(True)
ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=AZUL), plt.Rectangle((0, 0), 1, 1, color=NARANJA)],
          labels=["SortMergeJoin (autoBroadcastJoinThreshold = -1)", "BroadcastHashJoin (hint broadcast)"],
          frameon=False, loc="upper right")
fig.text(0.01, 0.01, "Fuente: evidencias/04_optimizacion.txt (Spark 3.2.1 local[16]; 44 541 interrupciones x 3 dimensiones; "
         "1 corrida de calentamiento + 3 medidas).", fontsize=8.5, color=TINTA2, ha="left", va="bottom")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(SALIDA, dpi=150)
print("PNG generado: graficos/g4_optimizacion.png")
