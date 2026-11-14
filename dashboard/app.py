"""Dashboard SEDALIB - riesgo de interrupcion del servicio de agua por distrito-mes (capa gold).

Local:  streamlit run dashboard/app.py
Datos:  dashboard/data/*.csv  (exportados de s3://<bucket>/curated/gold/ con aws/05_gold_athena.py --exportar)
"""
import os

import altair as alt
import pandas as pd
import streamlit as st

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
LL = "LA LIBERTAD"
UMBRAL = 0.1084
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"           # categoricos 1-3 (validados)
CRITICO, NEUTRO, TINTA2 = "#d03b3b", "#c9c8c3", "#52514e"       # estado critico = alerta
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
    for df in (h, t, k):
        df["periodo"] = df["anio"].astype(str) + "-" + df["mes"].map("{:02d}".format)
    return h, d, t, k, c


h, dims, tiempo, kpi, comp = cargar()
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


def val(df, c):
    return None if df.empty or pd.isna(df.iloc[0][c]) else float(df.iloc[0][c])


def pct(v):
    return "-" if v is None else "%.1f %%" % (100 * v)


# ---------------------------------------------------------------- 4 KPIs
st.subheader("%s - %s" % (depto.title(), etq[periodo]))
k1, k2, k3, k4 = st.columns(4)
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

# ---------------------------------------------------------------- ranking La Libertad + tendencia
c1, c2 = st.columns([1, 1.25])
with c1:
    st.markdown("**Ranking de distritos de La Libertad por probabilidad - %s**" % etq[periodo])
    r = h[(h["departamento"] == LL) & (h["periodo"] == periodo)].copy().sort_values("prob_interrupcion", ascending=False)
    r["estado"] = r["alerta"].map({1: "En alerta", 0: "Sin alerta"})
    r["real"] = r["interrupcion_real"].map({1: "si", 0: "no"})
    r["etiqueta"] = r.apply(lambda x: "%.1f %%%s" % (100 * x.prob_interrupcion, "  ALERTA" if x.alerta else ""), axis=1)
    base = alt.Chart(r).encode(y=alt.Y("distrito:N", sort=list(r["distrito"]), title=None,
                                       axis=alt.Axis(labelLimit=180, labelOverlap=False)))
    barras = base.mark_bar(cornerRadiusEnd=4, height=14).encode(
        x=alt.X("prob_interrupcion:Q", title="Probabilidad de interrupcion", axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("estado:N", scale=alt.Scale(domain=["En alerta", "Sin alerta"], range=[CRITICO, NEUTRO]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=[alt.Tooltip("distrito:N"), alt.Tooltip("prob_interrupcion:Q", title="probabilidad", format=".1%"),
                 alt.Tooltip("estado:N"), alt.Tooltip("real:N", title="interrupcion real")])
    texto = base.mark_text(align="left", dx=4, fontSize=11, color=TINTA2).encode(
        x="prob_interrupcion:Q", text="etiqueta:N")
    regla = alt.Chart(pd.DataFrame({"u": [UMBRAL]})).mark_rule(strokeDash=[4, 3], color=TINTA2).encode(x="u:Q")
    st.altair_chart((barras + texto + regla).properties(height=27 * max(len(r), 1) + 30), use_container_width=True)
    st.caption("Linea punteada = umbral %.4f. Rojo = en alerta." % UMBRAL)

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
    sel = alt.Chart(pd.DataFrame({"f": [pd.Timestamp(periodo + "-01")]})).mark_rule(color=TINTA2, strokeDash=[2, 2]).encode(x="f:T")
    hold = alt.Chart(pd.DataFrame({"a": [pd.Timestamp("2024-01-01")], "b": [pd.Timestamp("2024-05-01")]})).mark_rect(
        opacity=0.08, color=TINTA2).encode(x="a:T", x2="b:T")
    st.altair_chart((hold + lin + sel).properties(height=250), use_container_width=True)
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

with st.expander("Ver tablas"):
    st.dataframe(cc.drop(columns=["conjunto"]), hide_index=True, use_container_width=True)
    st.dataframe(kpi[kpi["departamento"] == depto].drop(columns=["periodo"]), hide_index=True, use_container_width=True)
    st.dataframe(r[["distrito", "ubigeo", "prob_interrupcion", "estado", "real"]], hide_index=True, use_container_width=True)
