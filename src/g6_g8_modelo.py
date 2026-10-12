"""g6, g7 y g8 del modelo base M1 (python src/g6_g8_modelo.py, en el host).

g6 y g8: cifras REALES de evidencias/06_analisis_modelo.txt (matrices de confusion, importancias).
g7: curvas ROC y PR calculadas con las probabilidades de prueba que exporta analisis_modelo.py
    (evidencias/modelo_m1_predicciones_test.csv); en la leyenda se muestran las AUC reportadas por Spark.
"""
import csv
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from sklearn.metrics import precision_recall_curve, roc_curve  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
GRAF = os.path.join(BASE, "graficos")
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
TINTA, TINTA2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})
FUENTE = "Fuente: evidencias/06_analisis_modelo.txt (Spark MLlib; variante M1, 323 distritos; prueba = 2024 ene-may, 1 615 distrito-meses)."

txt = open(os.path.join(EVID, "06_analisis_modelo.txt"), encoding="utf-8").read()
m1 = txt[txt.index("Variante M1"):txt.index("Variante M2")]
MODELOS = [("Regresion Logistica", "Regresión logística", AZUL), ("Arbol de Decision", "Árbol de decisión", NARANJA),
           ("Linea base ingenua (tasa hist.)", "Línea base ingenua", AQUA)]
met = {}
for clave, _, _ in MODELOS:
    m = re.search(re.escape(clave) + r"\s+AUC-ROC=([\d.]+)\s+AUC-PR=([\d.]+) \| TN=\s*(\d+) FP=\s*(\d+) FN=\s*(\d+) TP=\s*(\d+)", m1)
    met[clave] = dict(roc=float(m.group(1)), pr=float(m.group(2)), tn=int(m.group(3)), fp=int(m.group(4)),
                      fn=int(m.group(5)), tp=int(m.group(6)))
prev = re.search(r"test=(\d+) filas \(prevalencia ([\d.]+)\)", m1)


def pie(fig, texto=FUENTE):
    fig.text(0.01, 0.01, texto, fontsize=8.5, color=TINTA2, ha="left", va="bottom")


# ------------------------------------------------------------------ g6
print("g6 - Matrices de confusion M1 (umbral 0.5)")
fig, axs = plt.subplots(1, 3, figsize=(15, 5.3))
for ax, (clave, nombre, _) in zip(axs, MODELOS):
    d = met[clave]
    cm = np.array([[d["tn"], d["fp"]], [d["fn"], d["tp"]]])
    print("   %-22s TN=%d FP=%d FN=%d TP=%d" % (nombre, d["tn"], d["fp"], d["fn"], d["tp"]))
    ax.imshow(cm, cmap="Blues", vmin=0, vmax=cm.max() * 1.15)
    for i in range(2):
        for j in range(2):
            ax.text(j, i, "%s\n%d\n(%.1f %%)" % ([["VN", "FP"], ["FN", "VP"]][i][j], cm[i, j], cm[i, j] * 100 / cm.sum()),
                    ha="center", va="center", fontsize=11, color="white" if cm[i, j] > cm.max() * 0.6 else TINTA)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Sin interrupción", "Con interrupción"])
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["Sin interrupción", "Con interrupción"])
    ax.set_xlabel("Predicción (umbral 0,5)")
    ax.set_ylabel("Real")
    prec = d["tp"] / (d["tp"] + d["fp"])
    rec = d["tp"] / (d["tp"] + d["fn"])
    ax.set_title("%s\nprecisión %.3f · recall %.3f" % (nombre, prec, rec), fontsize=11.5, color=TINTA)
    for s in ax.spines.values():
        s.set_visible(False)
fig.suptitle("g6. Matrices de confusión del modelo base M1 (prevalencia en prueba %.4f)" % float(prev.group(2)),
             x=0.01, ha="left", fontsize=13, color=TINTA)
pie(fig)
fig.tight_layout(rect=(0, 0.04, 1, 0.93))
fig.savefig(os.path.join(GRAF, "g6_matriz_confusion.png"), dpi=150)
plt.close(fig)

# ------------------------------------------------------------------ g7
ruta = os.path.join(EVID, "modelo_m1_predicciones_test.csv")
with open(ruta, encoding="utf-8") as f:
    filas = list(csv.DictReader(f))
y = np.array([float(r["label"]) for r in filas])
scores = {"Regresion Logistica": "P_LR", "Arbol de Decision": "P_ARBOL", "Linea base ingenua (tasa hist.)": "P_INGENUA"}
print("g7 - Curvas ROC/PR desde %s (%d filas, prevalencia %.4f)" % (os.path.basename(ruta), len(y), y.mean()))
fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6))
for clave, nombre, color in MODELOS:
    s = np.array([float(r[scores[clave]]) for r in filas])
    fpr, tpr, _ = roc_curve(y, s)
    pr, rc, _ = precision_recall_curve(y, s)
    a1.plot(fpr, tpr, color=color, lw=2, label="%s (AUC-ROC %.4f)" % (nombre, met[clave]["roc"]))
    a2.plot(rc, pr, color=color, lw=2, label="%s (AUC-PR %.4f)" % (nombre, met[clave]["pr"]))
    print("   %-22s AUC-ROC Spark=%.4f AUC-PR Spark=%.4f" % (nombre, met[clave]["roc"], met[clave]["pr"]))
a1.plot([0, 1], [0, 1], ls=":", color=TINTA2, lw=1.2, label="Azar")
a2.axhline(y.mean(), ls=":", color=TINTA2, lw=1.2, label="Prevalencia (%.3f)" % y.mean())
for ax, t, xl, yl in [(a1, "Curva ROC", "Tasa de falsos positivos", "Tasa de verdaderos positivos (recall)"),
                      (a2, "Curva precisión-recall", "Recall", "Precisión")]:
    ax.set_title(t, loc="left", fontsize=12, color=TINTA)
    ax.set_xlabel(xl)
    ax.set_ylabel(yl)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.02)
    ax.grid(True, color=GRILLA)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="lower right" if ax is a1 else "lower left", fontsize=9.5)
fig.suptitle("g7. Curvas ROC y PR del modelo base M1 en el año de prueba", x=0.01, ha="left", fontsize=13, color=TINTA)
pie(fig, "Fuente: probabilidades de prueba en evidencias/modelo_m1_predicciones_test.csv (analisis_modelo.py); "
         "AUC de la leyenda = valores calculados por Spark en evidencias/06_analisis_modelo.txt.")
fig.tight_layout(rect=(0, 0.04, 1, 0.94))
fig.savefig(os.path.join(GRAF, "g7_roc_pr.png"), dpi=150)
plt.close(fig)

# ------------------------------------------------------------------ g8
imp = re.findall(r"^\s{3}(\w+)\s+([\d.]+)$", m1[m1.index("Importancia - Arbol"):m1.index("Coeficientes")], re.M)
coef = re.findall(r"^\s{3}(\w+)\s+([+-][\d.]+)$", m1[m1.index("Coeficientes"):], re.M)
print("g8 - Importancias (arbol) y coeficientes estandarizados (LR), M1")
print("   arbol:", imp)
print("   LR   :", coef)
fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6.2))
n, v = zip(*[(a, float(b)) for a, b in reversed(imp)])
bars = a1.barh(n, v, color=NARANJA, height=0.6)
for b in bars:
    a1.annotate("%.4f" % b.get_width(), (b.get_width(), b.get_y() + b.get_height() / 2), xytext=(4, 0),
                textcoords="offset points", va="center", fontsize=9, color=TINTA)
a1.set_xscale("symlog", linthresh=0.01)
a1.set_xlim(0, 3)
a1.set_title("Árbol de decisión: featureImportances (top 10)", loc="left", fontsize=12, color=TINTA)
a1.set_xlabel("Importancia (escala symlog; suman 1)")
n, v = zip(*[(a, float(b)) for a, b in reversed(coef)])
bars = a2.barh(n, v, color=[AZUL if x > 0 else AQUA for x in v], height=0.6)
for b in bars:
    w = b.get_width()
    a2.annotate("%+.4f" % w, (w, b.get_y() + b.get_height() / 2), xytext=(4 if w > 0 else -4, 0),
                textcoords="offset points", va="center", ha="left" if w > 0 else "right", fontsize=9, color=TINTA)
a2.axvline(0, color=TINTA2, lw=1)
a2.set_xscale("symlog", linthresh=0.1)
a2.set_xlim(-1, 30)
a2.set_title("Regresión logística: coef. estandarizados (top 10)", loc="left", fontsize=12, color=TINTA)
a2.set_xlabel("Coeficiente (escala symlog; azul = aumenta el riesgo)")
for ax in (a1, a2):
    ax.xaxis.grid(True, color=GRILLA)
    ax.set_axisbelow(True)
fig.suptitle("g8. Importancia de variables del modelo base M1 (solo variables rezagadas o estáticas)",
             x=0.01, ha="left", fontsize=13, color=TINTA)
pie(fig, FUENTE + " INT_PREV3 = interrupciones de los 3 meses previos.")
fig.tight_layout(rect=(0, 0.04, 1, 0.94))
fig.savefig(os.path.join(GRAF, "g8_importancia.png"), dpi=150)
plt.close(fig)
print("PNG generados: g6_matriz_confusion.png, g7_roc_pr.png, g8_importancia.png")
