"""g5b. Escalabilidad local (Docker) vs AWS (EMR Serverless) -> graficos/g5b_escalabilidad_local_vs_aws.png

python src/g5b_escalabilidad_local_vs_aws.py   (en el host, despues de aws/04_escalabilidad_emr.py)
Cifras leidas de evidencias/escalabilidad_spark.json (local) y evidencias/aws_04_escalabilidad.json (AWS).
Panel izquierdo: B = mismo subpipeline en ambos entornos (comparacion directa local vs AWS).
Panel derecho: A = procesamiento_spark.py completo en AWS (serie aparte, sin equivalente local).
Barras rayadas = fallo por memoria (altura = tiempo hasta el fallo).
"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
TINTA, TINTA2, GRILLA, SUPERFICIE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"          # slots categoricos 1-3 (validados)
ESCALAS = [100, 500, 1000]
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10.5, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})

loc = {r["escala"]: r for r in json.load(open(os.path.join(EVID, "escalabilidad_spark.json")))}
aws = json.load(open(os.path.join(EVID, "aws_04_escalabilidad.json")))
etq_x = ["%dX\n%s filas" % (n, "{:,}".format(45671 * n).replace(",", " ")) for n in ESCALAS]


def barra(ax, x, v, est, color, ancho, ymax, nota=None):
    if v is None:
        ax.text(x, ymax * 0.02, nota or "no\nejecutado", ha="center", va="bottom", fontsize=8.2, color=TINTA2)
        return
    if est == "OK":
        ax.bar(x, v, ancho, color=color, zorder=2)
        ax.text(x, v + ymax * 0.015, "%.1f s" % v, ha="center", va="bottom", fontsize=9, color=TINTA, fontweight="bold")
    else:
        ax.bar(x, v, ancho, facecolor=SUPERFICIE, edgecolor=color, hatch="///", linewidth=1.6, zorder=2)
        ax.text(x, v + ymax * 0.015, "%s\na los %.1f s" % (est, v), ha="center", va="bottom", fontsize=8.6, color=TINTA)


fig, (a1, a2) = plt.subplots(1, 2, figsize=(13.5, 6.4), gridspec_kw={"width_ratios": [1.45, 1]})
fig.patch.set_facecolor(SUPERFICIE)
fig.subplots_adjust(top=0.78, bottom=0.2, left=0.06, right=0.985, wspace=0.18)

# ---- panel B: local vs AWS (mismo subpipeline)
ymax = max([loc[n]["tiempo_s"] for n in ESCALAS if n in loc] + [aws["B"][k]["t"] for k in aws["B"]])
ancho = 0.36
for i, n in enumerate(ESCALAS):
    r = loc.get(n)
    barra(a1, i - 0.19, r["tiempo_s"] if r else None, ("OK" if r and r["estado"] == "OK" else "OOM"), AZUL, ancho, ymax)
    r = aws["B"].get(str(n))
    est = None if not r else ("OK" if r["estado"] == "OK" else ("OOM" if "OOM" in r["estado"] else "FALLO"))
    barra(a1, i + 0.19, r["t"] if r else None, est, NARANJA, ancho, ymax)
a1.set_ylim(0, ymax * 1.3)
a1.set_title("B: mismo subpipeline, local vs AWS", loc="left", fontsize=11.5, color=TINTA)
a1.legend(handles=[Patch(facecolor=AZUL, label="Local Docker (local[16], heap 1 GiB)"),
                   Patch(facecolor=NARANJA, label="AWS EMR Serverless (8 vCPU / 32 GB)"),
                   Patch(facecolor=SUPERFICIE, edgecolor=TINTA2, hatch="///", label="fallo por memoria (OOM)")],
          loc="upper left", fontsize=8.8, frameon=False)

# ---- panel A: procesamiento_spark.py completo en AWS
ymax2 = max([aws["A"][k]["script_s"] for k in aws["A"] if aws["A"][k]["script_s"]] + [1])
for i, n in enumerate(ESCALAS):
    r = aws["A"].get(str(n))
    if r:
        est = "OK" if r["estado"] == "SUCCESS" else ("OOM" if r["oom"] else "FALLO")
        barra(a2, i, r["script_s"] or r["dur_s"], est, AQUA, 0.5, ymax2)
    else:
        nota = "no ejecutado\n(costo y tiempo)" if n == 1000 and aws.get("nota_A1000") else None
        barra(a2, i, None, None, AQUA, 0.5, ymax2, nota)
a2.set_ylim(0, ymax2 * 1.3)
a2.set_title("A: procesamiento_spark.py completo, solo AWS", loc="left", fontsize=11.5, color=TINTA)

for ax in (a1, a2):
    ax.set_facecolor(SUPERFICIE)
    ax.set_xlim(-0.6, len(ESCALAS) - 0.4)
    ax.set_xticks(range(len(ESCALAS)))
    ax.set_xticklabels(etq_x, fontsize=9.2)
    ax.set_ylabel("Tiempo del script (s)")
    ax.grid(axis="y", color=GRILLA, lw=0.8, zorder=0)

fig.suptitle("g5b. Escalabilidad por réplica: local (Docker) vs AWS (EMR Serverless)", x=0.01, ha="left",
             fontsize=13.5, color=TINTA, fontweight="bold")
fig.text(0.01, 0.86, "B = dedup, normalizar, 3 JOINs, groupBy distrito-mes y write(noop); es la comparación directa. "
         "A = pipeline real completo,\nsin equivalente local (escala distinta: no comparar alturas entre paneles). "
         "Réplicas solo para medir rendimiento: no alimentan el modelo.", fontsize=9.2, color=TINTA2)
fig.text(0.01, 0.015, "Fuente: evidencias/escalabilidad_spark.json (local, Spark 3.2.1) y "
         "evidencias/aws_04_escalabilidad.json (EMR 7.9.0, Spark 3.5.5, us-east-2).\nRéplicas: crossJoin(spark.range(N)) "
         "sobre 45 671 interrupciones SUNASS.", fontsize=8.2, color=TINTA2)
fig.savefig(os.path.join(BASE, "graficos", "g5b_escalabilidad_local_vs_aws.png"), dpi=150, facecolor=SUPERFICIE)
print("graficos/g5b_escalabilidad_local_vs_aws.png generado")
