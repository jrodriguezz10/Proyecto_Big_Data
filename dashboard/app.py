"""Dashboard SEDALIB - riesgo de interrupcion del servicio de agua por distrito-mes (capa gold).

Local:  streamlit run dashboard/app.py          (?vista=plan abre directo en el plan de accion)
Datos:  dashboard/data/*.csv  (exportados de s3://<bucket>/curated/gold/ con aws/05_gold_athena.py --exportar)
        gold_impacto_distrito.csv y tasa_historica_distrito_mes.csv: src/gold_impacto.py
"""
import os

import altair as alt
import pandas as pd
import streamlit as st

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
LL = "LA LIBERTAD"
UMBRAL = 0.1084
UMBRAL_INSPECCION = 0.50
RESPONSABLE = "Jefatura de Mantenimiento de Redes"
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"           # categoricos 1-3 (validados)
CRITICO, NEUTRO, TINTA2, TINTA = "#d03b3b", "#c9c8c3", "#52514e", "#0b0b0b"   # estado critico = alerta
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]

st.set_page_config(page_title="SEDALIB - riesgo de interrupcion", layout="wide")
st.markdown("<style>.block-container{padding-top:1.6rem;padding-bottom:1rem}</style>", unsafe_allow_html=True)


@st.cache_data
def cargar():
    h = pd.read_csv(os.path.join(DATA, "gold_riesgo_distrito_mes.csv"), dtype={"ubigeo": str})
    d = pd.read_csv(os.path.join(DATA, "dim_distrito.csv"), dtype={"ubigeo": str})
    t = pd.read_csv(os.path.join(DATA, "dim_tiempo.csv"))
    k = pd.read_csv(os.path.join(DATA, "gold_kpi_mensual.csv"))
    c = pd.read_csv(os.path.join(DATA, "gold_comparacion_modelos.csv"))
    imp = pd.read_csv(os.path.join(DATA, "gold_impacto_distrito.csv"), dtype={"ubigeo": str})
    th = pd.read_csv(os.path.join(DATA, "tasa_historica_distrito_mes.csv"))
    n = len(h)
    h = (h.merge(th[["departamento", "distrito", "anio", "mes", "prob_tasa_historica"]],
                 on=["departamento", "distrito", "anio", "mes"], how="left")
         .merge(imp[["departamento", "distrito", "conexiones_prom_mes", "unidades_uso_prom_mes"]],
                on=["departamento", "distrito"], how="left"))
    assert len(h) == n, "explosion de filas al unir impacto / tasa historica"
    h["conexiones_en_riesgo"] = h["conexiones_prom_mes"].where(h["alerta"] == 1, 0.0)
    for df in (h, t, k):
        df["periodo"] = df["anio"].astype(str) + "-" + df["mes"].map("{:02d}".format)
    return h, d, t, k, c


def accion(p):
    if p >= UMBRAL_INSPECCION:
        return "Inspeccion preventiva de redes"
    return "Monitoreo reforzado" if p >= UMBRAL else "Sin accion"


def miles(v):
    return "-" if pd.isna(v) else "{:,.0f}".format(v).replace(",", " ")


h, dims, tiempo, kpi, comp = cargar()
vista_plan = st.query_params.get("vista") == "plan"
periodos = sorted(tiempo["periodo"])
etq = {p: "%s %s" % (MESES[int(p[5:]) - 1], p[:4]) for p in periodos}

st.title("Riesgo de interrupcion del servicio de agua - capa gold")
st.caption("Modelo M1-GBT, probabilidad fuera de muestra (2022-01 a 2024-05). Alerta = probabilidad >= %.4f. "
           "Fuente: s3://<bucket>/curated/gold/ (Glue: sedalib_gold). Sin datos personales." % UMBRAL)

# ---------------------------------------------------------------- filtros (una fila)
f1, f2, _ = st.columns([1.2, 1, 2])
deptos = ["NACIONAL"] + sorted(h["departamento"].unique())
depto = f1.selectbox("Departamento", deptos, index=deptos.index(LL))
periodo = f2.selectbox("Mes", periodos, index=periodos.index("2024-04"), format_func=etq.get)

fila = kpi[(kpi["departamento"] == depto) & (kpi["periodo"] == periodo)]
ant = periodos[periodos.index(periodo) - 1] if periodos.index(periodo) > 0 else None
fila_ant = kpi[(kpi["departamento"] == depto) & (kpi["periodo"] == ant)] if ant else fila.iloc[0:0]
sel = h if depto == "NACIONAL" else h[h["departamento"] == depto]
mes_sel, mes_ant = sel[sel["periodo"] == periodo], sel[sel["periodo"] == ant]
_auc = comp.set_index(["ambito", "modelo"])["auc_pr"]
NOTA_LL = ("En La Libertad la tasa historica tuvo mayor AUC-PR que el modelo en el holdout 2024 (%.4f frente a %.4f): "
           "usala como segunda opinion antes de descartar un distrito."
           % (_auc[(LL, "Tasa historica")], _auc[(LL, "M1-GBT")]))


def val(df, c):
    return None if df.empty or pd.isna(df.iloc[0][c]) else float(df.iloc[0][c])


def pct(v):
    return "-" if v is None else "%.1f %%" % (100 * v)


# ---------------------------------------------------------------- plan de accion
def plan_accion():
    st.subheader("Plan de accion del mes - %s, %s" % (depto.title(), etq[periodo]))
    pl = mes_sel.sort_values("prob_interrupcion", ascending=False)
    nombre = pl["distrito"].str.title()
    if depto == "NACIONAL":
        nombre = nombre + " (" + pl["departamento"].str.title() + ")"
    tabla = pd.DataFrame({
        "Distrito": nombre.values,
        "Prob. modelo M1-GBT": pl["prob_interrupcion"].values,
        "Prob. tasa historica": pl["prob_tasa_historica"].values,
        "Conexiones en riesgo": pl["conexiones_en_riesgo"].round(0).values,
        "Accion sugerida": pl["prob_interrupcion"].map(accion).values,
        "Responsable": RESPONSABLE})
    colores = {"Inspeccion preventiva de redes": "background-color:#f6d5d5;color:#7a1f1f;font-weight:600",
               "Monitoreo reforzado": "background-color:#fbe3d6;color:#7a3a14", "Sin accion": "color:#52514e"}
    st.dataframe(tabla.style.map(lambda v: colores.get(v, ""), subset=["Accion sugerida"])
                 .format({"Prob. modelo M1-GBT": "{:.1%}", "Prob. tasa historica": "{:.1%}",
                          "Conexiones en riesgo": miles}),
                 hide_index=True, use_container_width=True, height=min(38 + 35 * len(tabla), 560))
    b1, b2 = st.columns([1, 4])
    b1.download_button("Descargar plan (CSV)", tabla.round(4).to_csv(index=False).encode("utf-8-sig"),
                       file_name="plan_accion_%s_%s.csv" % (depto.lower().replace(" ", "_"), periodo),
                       mime="text/csv", type="primary")
    n = tabla["Accion sugerida"].value_counts()
    b2.caption("Inspeccion preventiva: p >= %.2f (%d distritos) | monitoreo reforzado: %.4f <= p < %.2f (%d) | "
               "sin accion (%d). Conexiones en riesgo = promedio mensual de conexiones domiciliarias afectadas "
               "(SUNASS 2022-2023) si el distrito esta en alerta; 0 si no. %s"
               % (UMBRAL_INSPECCION, n.get("Inspeccion preventiva de redes", 0), UMBRAL, UMBRAL_INSPECCION,
                  n.get("Monitoreo reforzado", 0), n.get("Sin accion", 0),
                  NOTA_LL if depto in (LL, "NACIONAL") else ""))


if vista_plan:
    plan_accion()

# ---------------------------------------------------------------- 5 KPIs
st.subheader("%s - %s" % (depto.title(), etq[periodo]))
k1, k2, k3, k4, k5 = st.columns(5)
al, al0 = val(fila, "distritos_en_alerta"), val(fila_ant, "distritos_en_alerta")
nd = val(fila, "n_distritos")
k1.metric("Distritos en alerta", "-" if al is None else "%d de %d" % (al, nd),
          None if al is None or al0 is None else "%+d vs mes anterior" % (al - al0), delta_color="inverse")
ta, ta0 = val(fila, "tasa_interrupcion"), val(fila_ant, "tasa_interrupcion")
k2.metric("Tasa de interrupcion real", pct(ta),
          None if ta is None or ta0 is None else "%+.1f pp vs mes anterior" % (100 * (ta - ta0)), delta_color="inverse")
k3.metric("Precision de las alertas", pct(val(fila, "precision_alertas")),
          help="De los distritos en alerta, cuantos tuvieron interrupcion (TP / (TP + FP)).")
k4.metric("Recall de las alertas", pct(val(fila, "recall_alertas")),
          help="De los distritos con interrupcion, cuantos estaban en alerta (TP / (TP + FN)).")
cr, cr0 = mes_sel["conexiones_en_riesgo"].sum(), mes_ant["conexiones_en_riesgo"].sum()
sin_dato = int(((mes_sel["alerta"] == 1) & mes_sel["conexiones_prom_mes"].isna()).sum())
k5.metric("Conexiones en riesgo este mes", miles(cr),
          None if ant is None else "{:+,.0f} vs mes anterior".format(cr - cr0).replace(",", " "), delta_color="inverse",
          help="Suma, en los distritos en alerta, del promedio mensual de conexiones domiciliarias afectadas por "
               "interrupciones (SUNASS 2022-2023). Cuenta afectaciones: una conexion afectada dos veces en un mes "
               "suma dos. Distritos en alerta sin dato de conexiones: %d." % sin_dato)

# ---------------------------------------------------------------- ranking La Libertad + tendencia
c1, c2 = st.columns([1, 1.25])
with c1:
    st.markdown("**Ranking de distritos de La Libertad: modelo y tasa historica - %s**" % etq[periodo])
    r = h[(h["departamento"] == LL) & (h["periodo"] == periodo)].copy().sort_values("prob_interrupcion", ascending=False)
    r["estado"] = r["alerta"].map({1: "En alerta", 0: "Sin alerta"})
    r["real"] = r["interrupcion_real"].map({1: "si", 0: "no"})
    r["etiqueta"] = r.apply(lambda x: "%.1f %%%s" % (100 * x.prob_interrupcion, "  ALERTA" if x.alerta else ""), axis=1)
    r["x_etq"] = r[["prob_interrupcion", "prob_tasa_historica"]].max(axis=1)
    tt = [alt.Tooltip("distrito:N"), alt.Tooltip("prob_interrupcion:Q", title="prob. modelo", format=".1%"),
          alt.Tooltip("prob_tasa_historica:Q", title="prob. tasa historica", format=".1%"),
          alt.Tooltip("estado:N"), alt.Tooltip("real:N", title="interrupcion real")]
    base = alt.Chart(r).encode(y=alt.Y("distrito:N", sort=list(r["distrito"]), title=None,
                                       axis=alt.Axis(labelLimit=180, labelOverlap=False)))
    barras = base.mark_bar(cornerRadiusEnd=4, height=14).encode(
        x=alt.X("prob_interrupcion:Q", title="Probabilidad de interrupcion", axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("estado:N", scale=alt.Scale(domain=["En alerta", "Sin alerta"], range=[CRITICO, NEUTRO]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=tt)
    tasa_m = base.mark_tick(color=TINTA, thickness=3, size=20).encode(x="prob_tasa_historica:Q", tooltip=tt)
    texto = base.mark_text(align="left", dx=6, fontSize=11, color=TINTA2).encode(x="x_etq:Q", text="etiqueta:N")
    regla = alt.Chart(pd.DataFrame({"u": [UMBRAL]})).mark_rule(strokeDash=[4, 3], color=TINTA2).encode(x="u:Q")
    st.altair_chart((barras + regla + tasa_m + texto).properties(height=27 * max(len(r), 1) + 30),
                    use_container_width=True)
    st.caption("Barra = probabilidad del modelo (rojo = en alerta). Marca negra = probabilidad por tasa historica. "
               "Linea punteada = umbral %.4f. %s" % (UMBRAL, NOTA_LL))

with c2:
    st.markdown("**Tendencia mensual - %s: distritos en alerta frente a distritos con interrupcion real**" % depto.title())
    tk = kpi[kpi["departamento"] == depto].sort_values("periodo")
    larga = tk.melt(id_vars=["periodo"], value_vars=["distritos_en_alerta", "distritos_con_interrupcion"],
                    var_name="serie", value_name="distritos")
    larga["serie"] = larga["serie"].map({"distritos_en_alerta": "Alertas (modelo)",
                                         "distritos_con_interrupcion": "Interrupciones reales"})
    larga["fecha"] = pd.to_datetime(larga["periodo"] + "-01")
    esc = alt.Scale(domain=["Alertas (modelo)", "Interrupciones reales"], range=[AZUL, NARANJA])
    lin = alt.Chart(larga).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=60, filled=True)).encode(
        x=alt.X("fecha:T", title=None, axis=alt.Axis(format="%b %Y", labelAngle=0, tickCount=8)),
        y=alt.Y("distritos:Q", title="Distritos"),
        color=alt.Color("serie:N", scale=esc, legend=alt.Legend(title=None, orient="top")),
        tooltip=[alt.Tooltip("periodo:N", title="mes"), alt.Tooltip("serie:N"), alt.Tooltip("distritos:Q")])
    sel_mes = alt.Chart(pd.DataFrame({"f": [pd.Timestamp(periodo + "-01")]})).mark_rule(
        color=TINTA2, strokeDash=[2, 2]).encode(x="f:T")
    hold = alt.Chart(pd.DataFrame({"a": [pd.Timestamp("2024-01-01")], "b": [pd.Timestamp("2024-05-01")]})).mark_rect(
        opacity=0.08, color=TINTA2).encode(x="a:T", x2="b:T")
    st.altair_chart((hold + lin + sel_mes).properties(height=250), use_container_width=True)
    st.caption("Franja gris = holdout 2024 (nunca visto al elegir el modelo). Linea vertical = mes seleccionado. "
               "2024-05 esta incompleto en el origen (SUNASS termina el 2024-05-07).")

with c2:
    st.markdown("**Comparacion del modelo frente a la tasa historica - holdout 2024-01..05**")
    ambito = LL if depto == LL else "NACIONAL"
    cc = comp[comp["ambito"] == ambito]
    larga = cc.melt(id_vars=["modelo"], value_vars=["auc_pr", "auc_roc", "precision", "recall"],
                    var_name="metrica", value_name="valor")
    larga["metrica"] = larga["metrica"].map({"auc_pr": "AUC-PR", "auc_roc": "AUC-ROC", "precision": "Precision",
                                             "recall": "Recall"})
    orden = ["M1-GBT", "Tasa historica", "Regla ingenua"]
    g = alt.Chart(larga).mark_bar(cornerRadiusEnd=4).encode(
        x=alt.X("modelo:N", sort=orden, title=None, axis=alt.Axis(labels=False, ticks=False)),
        y=alt.Y("valor:Q", title=None, scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format=".0%")),
        color=alt.Color("modelo:N", sort=orden, scale=alt.Scale(domain=orden, range=[AZUL, NARANJA, AQUA]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=[alt.Tooltip("modelo:N"), alt.Tooltip("metrica:N"), alt.Tooltip("valor:Q", format=".4f")])
    t = g.mark_text(dy=-7, fontSize=11).encode(text=alt.Text("valor:Q", format=".2f"), color=alt.value(TINTA2))
    st.altair_chart(alt.layer(g, t).properties(width=115, height=150).facet(
        column=alt.Column("metrica:N", title=None, sort=["AUC-PR", "AUC-ROC", "Precision", "Recall"])),
        use_container_width=False)
    fila_g = cc[cc["modelo"] == "M1-GBT"].iloc[0]
    fila_t = cc[cc["modelo"] == "Tasa historica"].iloc[0]
    st.caption("Ambito: %s (n=%d distrito-mes, %d con interrupcion). AUC-PR M1-GBT %.4f vs tasa historica %.4f (%+.4f). "
               "Umbrales: M1-GBT %.4f, tasa historica %.4f, regla ingenua 1."
               % (ambito.title(), fila_g.n, fila_g.positivos, fila_g.auc_pr, fila_t.auc_pr, fila_g.auc_pr - fila_t.auc_pr,
                  fila_g.umbral, fila_t.umbral))

if not vista_plan:
    plan_accion()

with st.expander("Ver tablas"):
    st.dataframe(cc.drop(columns=["conjunto"]), hide_index=True, use_container_width=True)
    st.dataframe(kpi[kpi["departamento"] == depto].drop(columns=["periodo"]), hide_index=True, use_container_width=True)
    st.dataframe(r[["distrito", "ubigeo", "prob_interrupcion", "prob_tasa_historica", "conexiones_prom_mes", "estado",
                    "real"]], hide_index=True, use_container_width=True)
