"""Vistas de terminal de evidencias locales para las capturas del informe (solo lee archivos de evidencias/).

    python aws/ver_evidencias.py modelo    -> evidencias/04_modelo_definitivo.txt: AUC-PR por corte y modelo + umbral
    python aws/ver_evidencias.py impacto   -> evidencias/06_impacto_distrito.txt: % de nulos y distritos con dato
"""
import os
import re
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def leer(nombre):
    with open(os.path.join(BASE, "evidencias", nombre), encoding="utf-8") as f:
        return f.read().splitlines()


def modelo():
    L = leer("04_modelo_definitivo.txt")
    print("evidencias/04_modelo_definitivo.txt - validacion rolling-origin (4 cortes semestrales) + holdout 2024\n")
    cortes, auc, corte = [], {}, None
    for l in L:
        m = re.match(r"Corte (\S+) \(", l.strip())
        if m:
            corte = m.group(1)
            cortes.append(corte)
            continue
        m = re.match(r"\s+(M[12])\s+(.+?)\s+AUC-PR=([\d.]+)", l)
        if m and corte:
            auc.setdefault((m.group(1), m.group(2).strip()), {})[corte] = float(m.group(3))
        if "4.2 RESUMEN" in l:
            corte = None
    resumen = {}
    for l in L:
        m = re.match(r"(M[12])\s{2}(\S.*?)\s{2,}([\d.]+)\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+([\d.]+)$", l)
        if m:
            resumen[(m.group(1), m.group(2).strip())] = (float(m.group(3)), float(m.group(4)), float(m.group(5)))
    print("AUC-PR nacional por corte de validacion (M1 = sin clima, M2 = con clima)")
    print("%-4s %-20s" % ("var", "modelo") + "".join("%10s" % c for c in cortes) + "%10s %8s %10s" % ("media", "desv", "media_LL"))
    print("-" * (25 + 10 * len(cortes) + 30))
    for k in sorted(auc, key=lambda k: -resumen.get(k, (0,))[0]):
        r = resumen.get(k, (0, 0, 0))
        marca = "  <== seleccionado" if k == ("M1", "GBT") else ""
        print("%-4s %-20s" % k + "".join("%10.4f" % auc[k][c] for c in cortes) + "%10.4f %8.4f %10.4f%s" % (*r, marca))
    print("\nUmbral elegido en validacion agrupada (recall maximo con precision >= 0.60)")
    en = False
    for l in L:
        if "4.3 UMBRAL" in l:
            en = True
            continue
        if en and "4.4 HOLDOUT" in l:
            break
        if en and l.strip() and not l.startswith("="):
            print(l + ("   <== M1-GBT: umbral 0.110" if l.startswith("M1  GBT") else ""))
    print("\nHoldout 2024-01..05 (nunca visto al elegir), modelo seleccionado:")
    amb = None
    for l in L:
        if l.startswith("--- Ambito:"):
            amb = l.split("|")[0].replace("--- Ambito:", "").strip()
        if l.startswith("M1  GBT") and "seleccionado" in l:
            c = l.split()
            print("  %-12s AUC-PR %s | AUC-ROC %s | umbral %s | precision %s | recall %s" % (amb, c[2], c[3], c[4], c[5], c[6]))


def impacto():
    L = leer("06_impacto_distrito.txt")
    print("evidencias/06_impacto_distrito.txt - conexiones afectadas por distrito (SUNASS)\n")
    for l in L:
        if l.startswith("=") or not l.strip():
            continue
        if l.startswith("La Libertad:"):
            break
        print(l)
    print("\nLa Libertad (promedio mensual de afectados en meses con interrupcion y dato valido):")
    print("%-22s %12s %12s %12s %6s" % ("distrito", "conexiones", "unid. uso", "mayor evento", "meses"))
    en = False
    for l in L:
        if l.strip().startswith("distrito") and "conexiones_prom_mes" in l:
            en = True
            continue
        if en:
            c = l.split()
            if len(c) < 6 or not re.match(r"[\d.]+$", c[-1]):
                break
            nombre = " ".join(c[:-5])
            print("%-22s %12s %12s %12s %6s" % (nombre, c[-5], c[-4], c[-2], c[-1]))


if __name__ == "__main__":
    {"modelo": modelo, "impacto": impacto}[sys.argv[1]]()
