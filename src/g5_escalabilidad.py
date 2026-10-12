"""g5. Escalabilidad Spark vs pandas (python src/g5_escalabilidad.py, en el host).

Lee las cifras REALES de evidencias/05_escalabilidad.txt (tabla final y lineas '->') y
evidencias/05b_pandas_escalabilidad.txt (tiempo hasta el fallo de pandas). Sin valores a mano.
"""
import ast
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVID = os.path.join(BASE, "evidencias")
SALIDA = os.path.join(BASE, "graficos", "g5_escalabilidad.png")
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
TINTA, TINTA2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.edgecolor": TINTA2,
                     "axes.labelcolor": TINTA, "xtick.color": TINTA2, "ytick.color": TINTA2,
                     "axes.spines.top": False, "axes.spines.right": False})


def num(x):
    try:
        return float(x)
    except ValueError:
        return None


# --- Spark: tabla final  "100X  4,567,100 |  25.87  1955  3737.4  388.1 |  -  -  OOM (...)"
txt = open(os.path.join(EVID, "05_escalabilidad.txt"), encoding="utf-8").read()
tabla = txt[txt.index("TABLA FINAL"):]
filas = []
for m in re.finditer(r"^(\d+)X\s+([\d,]+) \|\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+) \|\s+(\S+)\s+(\S+) (.*)$", tabla, re.M):
    filas.append({"escala": int(m.group(1)), "filas": int(m.group(2).replace(",", "")),
                  "spark_s": num(m.group(3)), "spark_estado": "OOM" if m.group(3) == "OOM" else ("OK" if num(m.group(3)) else "-"),
                  "mem": num(m.group(4)), "spill_mem": num(m.group(5)), "spill_disk": num(m.group(6)),
                  "pandas_s": num(m.group(7)), "pandas_estado": m.group(9).strip()})
# tiempo hasta el OOM de Spark (linea '->')
for m in re.finditer(r"^->\s+(\d+)X \|.*?\|\s+([\d.]+) s \|.*\| OOM", txt, re.M):
    for f in filas:
        if f["escala"] == int(m.group(1)):
            f["spark_s_fallo"] = float(m.group(2))
# tiempo hasta el OOM de pandas
for l in open(os.path.join(EVID, "05b_pandas_escalabilidad.txt"), encoding="utf-8"):
    if l.startswith("->"):
        d = ast.literal_eval(l[l.index("{"):].strip())
        for f in filas:
            if f["escala"] == d["escala"] and "tiempo_hasta_fallo_s" in d:
                f["pandas_s_fallo"] = d["tiempo_hasta_fallo_s"]
quiebre = dict(re.findall(r"PUNTO DE QUIEBRE (\w+)\s*: (\S+)", txt))

print("g5 - Escalabilidad (fuente: evidencias/05_escalabilidad.txt y 05b_pandas_escalabilidad.txt)")
for f in filas:
    print("   %5dX filas=%10d | Spark: %s %s s, mem %s MiB, spill mem %s MiB, spill disco %s MiB | pandas: %s %s s"
          % (f["escala"], f["filas"], f["spark_estado"], f.get("spark_s") or f.get("spark_s_fallo"), f["mem"],
             f["spill_mem"], f["spill_disk"], f["pandas_estado"][:3], f.get("pandas_s") or f.get("pandas_s_fallo")))
print("   Punto de quiebre: %s" % quiebre)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6.2), gridspec_kw={"width_ratios": [1.35, 1]})

# ---- panel 1: tiempo vs escala
for motor, color, clave, fallo, est in [("Spark (heap 1 GiB)", AZUL, "spark_s", "spark_s_fallo", "spark_estado"),
                                       ("pandas (contenedor 4 GiB)", NARANJA, "pandas_s", "pandas_s_fallo", "pandas_estado")]:
    ok = [f for f in filas if f[clave] is not None]
    ax1.plot([f["escala"] for f in ok], [f[clave] for f in ok], "-o", color=color, lw=2, ms=8, label=motor)
    for f in ok:
        # pandas: etiqueta debajo; Spark: arriba, salvo donde pandas marca OOM (abajo a la derecha)
        choca = clave == "spark_s" and any(g["escala"] == f["escala"] and g.get("pandas_s_fallo") for g in filas)
        off = (0, -17) if clave == "pandas_s" else ((30, -14) if choca else (0, 9))
        ax1.annotate("%.2f s" % f[clave], (f["escala"], f[clave]), xytext=off, textcoords="offset points",
                     ha="center", fontsize=9, color=TINTA)
    oom = [f for f in filas if f.get(fallo) is not None and f[est].startswith("OOM")]
    for f in oom:
        ax1.plot([ok[-1]["escala"], f["escala"]], [ok[-1][clave], f[fallo]], "--", color=color, lw=1.5)
        ax1.plot(f["escala"], f[fallo], "X", color=color, ms=13, mec="white", mew=1.5)
        ax1.annotate("OOM a los %.2f s\n(%s filas)" % (f[fallo], "{:,}".format(f["filas"]).replace(",", " ")),
                     (f["escala"], f[fallo]), xytext=(0, 12), textcoords="offset points", ha="center",
                     fontsize=9, color=TINTA, fontweight="bold")
for motor, color in [("pandas", NARANJA), ("Spark", AZUL)]:
    q = quiebre.get(motor)
    if q and q.endswith("X"):
        ax1.axvline(int(q[:-1]), color=color, ls=":", lw=1.4)
        ax1.text(int(q[:-1]), 0.98 if motor == "pandas" else 0.91, "quiebre %s: %s " % (motor, q),
                 transform=ax1.get_xaxis_transform(), color=color, fontsize=9.5, va="top", ha="right")
ax1.set_xscale("log")
ax1.set_yscale("log")
escalas = [f["escala"] for f in filas if f["escala"] <= 500]
ax1.set_xticks(escalas)
ax1.set_xticklabels(["%dX" % e for e in escalas])
ax1.set_xlim(0.6, 900)
ax1.set_ylim(0.15, 400)
ax1.set_xlabel("Escala del dataset de interrupciones (réplicas)")
ax1.set_ylabel("Tiempo del pipeline (s, escala log)")
ax1.set_title("Tiempo vs. escala", loc="left", fontsize=12, color=TINTA)
ax1.grid(True, which="major", color=GRILLA)
ax1.set_axisbelow(True)
ax1.legend(frameon=False, loc="lower right")

# ---- panel 2: spill de Spark
sp = [f for f in filas if f["spill_disk"] is not None]
x = range(len(sp))
w = 0.38
b1 = ax2.bar([i - w / 2 for i in x], [f["spill_mem"] for f in sp], w - 0.03, color=AQUA, label="Spill en memoria")
b2 = ax2.bar([i + w / 2 for i in x], [f["spill_disk"] for f in sp], w - 0.03, color=AZUL, label="Spill a disco")
for b in list(b1) + list(b2):
    ax2.annotate("%.1f" % b.get_height(), (b.get_x() + b.get_width() / 2, b.get_height()), xytext=(0, 3),
                 textcoords="offset points", ha="center", fontsize=9, color=TINTA)
ax2.set_xticks(list(x))
ax2.set_xticklabels(["%dX%s" % (f["escala"], "\n(OOM; última\nlectura API)" if f["spark_estado"] == "OOM" else "")
                     for f in sp])
ax2.set_ylabel("MiB")
ax2.set_title("Spill de Spark (API REST :4040)", loc="left", fontsize=12, color=TINTA)
ax2.yaxis.grid(True, color=GRILLA)
ax2.set_axisbelow(True)
ax2.set_ylim(0, max(f["spill_mem"] for f in sp) * 1.15)
ax2.legend(frameon=False, loc="upper left")

fig.suptitle("g5. Escalabilidad del pipeline: Spark vs. pandas y punto de quiebre", x=0.01, ha="left",
             fontsize=13, color=TINTA)
fig.text(0.01, 0.01, "Fuente: evidencias/05_escalabilidad.txt y 05b_pandas_escalabilidad.txt. Spark 3.2.1 local[16]; "
         "1000X no se ejecutó (la prueba se detiene en el primer OOM). Datos replicados solo para esta prueba.",
         fontsize=8.5, color=TINTA2, ha="left", va="bottom")
fig.tight_layout(rect=(0, 0.03, 1, 0.95))
fig.savefig(SALIDA, dpi=150)
print("PNG generado: graficos/g5_escalabilidad.png")
