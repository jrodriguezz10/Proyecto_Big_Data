# Resumen de resultados — Datalake SEDALIB

Todas las cifras salen de ejecuciones reales; la fuente de cada una está en `evidencias/NN_*.txt`. Las capturas `capNN` corresponden a esas mismas ejecuciones.
Entorno: Docker en Windows 11; HDFS 3.2.1 (namenode + datanode); Spark 3.2.1 en modo local[16]
dentro de `spark_node` (heap del driver de 1 GiB, valor por defecto).

## Avisos sobre los datos (léelos antes de redactar)

1. **SENAMHI no tiene estaciones en La Libertad.** El archivo trae solo 5 estaciones (Arequipa: Imata
   y San José de Uzuña; Lima-provincias: Matucana y Carania; Tacna: Candarave). Ningún distrito coincide
   con los de interrupciones (0 %). El clima se usó como promedio del **departamento** y solo existe
   para LIMA, AREQUIPA y TACNA. Por eso g2 es nacional (sin La Libertad) y el modelo con clima (M2)
   no incluye La Libertad.
2. **`sunass_anf.csv` no trae año ni volumen**: solo el porcentaje de ANF.
3. **DATASS no trae la edad en años**: solo la categoría "≤20 / >20 años" por sistema. La "antigüedad
   promedio" se expresa como % de sistemas con más de 20 años.
4. La Libertad tiene 13 distritos con interrupciones y 514 interrupciones en el panel; por eso g3 es
   nacional con La Libertad resaltada.
5. Los datos replicados (10X–1000X) se usan **solo** en la prueba de escalabilidad (sección 3.7).

---

## 1.4 Línea base (`01_linea_base.txt`)

| Indicador | Valor |
|---|---|
| ANF de SEDALIB S.A. | **38,00 %** (año: no existe en el archivo; volumen ANF: no existe) |
| ANF promedio de las 16 EPS del archivo | 41,59 % (mín. 29,50 %, máx. 60,79 %) |
| Interrupciones SEDALIB (eventos) | 516 en total: 2020 = 12, 2021 = 7, 2022 = 140, 2023 = 245, 2024 = 112 (solo ene–may) |
| Mediana de TIEMPO_RESPUESTA_DIAS (FECHAFIN − FECHAINICIO) | **0 días** (media 0,618; máx. 33) |
| Mediana de duración con horas | **14,67 h** (media 18,77 h) |
| Programadas / imprevistas | 327 (63,37 %) / 189 (36,63 %) |
| Motivo más frecuente | OTROS (276), Limpieza y desinfección de reservorio/cisterna (100), Rotura de tubería (70) |

SEDALIB representa el 1,13 % de las filas del archivo nacional de SUNASS (516 de 45 671).

## 1.7 Punto de quiebre (resumen de 3.7)

- **pandas** (contenedor con 4 GiB): funciona en 1X y 10X y **falla por memoria en 100X** (4 567 100 filas;
  el kernel mata el proceso con SIGKILL a los 32,77 s).
- **Spark** (local, heap de 1 GiB): completa 100X con spill (3 737,4 MiB en memoria y 388,1 MiB en disco)
  y **falla por OutOfMemoryError en 500X** (22 835 500 filas, a los 66,41 s). 1000X no se ejecutó
  porque la prueba se detiene en el primer OOM.
- Spark completa 10 veces más datos que pandas (100X frente a 10X) con un heap de 1 GiB (pico del contenedor
  1 955 MiB) frente a los 4 GiB de pandas, gracias al spill a disco.

## 3.3 Diagnóstico y procesamiento (`00_diagnostico.txt`, `02_procesamiento.txt`)

**Por qué salían resultados erróneos en la versión anterior**

| Síntoma anterior | Causa comprobada |
|---|---|
| Top distritos = Lima (S.J. de Lurigancho 3 988, V.M. del Triunfo 2 976, …) | No se filtraba por EPS/departamento: SEDAPAL tiene 29 369 filas (64,31 %) y el departamento de Lima 27 952 (61,2 %) |
| PP = 12 mm constante | El JOIN por DISTRITO con clima encontró **0** filas (0 % de coincidencia); `fillna("12.5")` rellenó las 45 671 filas y el cast a int dio 12 |
| 100 % "Sismo/Huaico" | INGEMMET usa Tipo Título ("La Molina") y SUNASS MAYÚSCULAS: 0 coincidencias; `fillna("Sismo/Huaico")` (que no es una categoría de INGEMMET) rellenó el 100 % |
| JOIN por fecha sin efecto | Se parseaba "20240418" con 'yyyy-MM-dd' → FECHA_CORTE nula en 45 671 de 45 671 filas |
| Explosión de filas | Con distritos normalizados, el JOIN 1-a-N con peligros pasaba de 45 671 a **3 730 914** filas |

**Fuentes raw (filas × columnas):** interrupciones 45 671 × 16; ANF 16 × 4; hidrojets 9 690 × 15;
clima 416 273 × 16; peligros 37 895 × 18 (de 189 en origen); DATASS antigüedad 21 171 × 14;
DATASS estado 11 444 × 14. PP en SENAMHI: 150 valores distintos, rango 0,0–32,5 mm por hora,
14 440 nulos y 378 897 ceros.

**Coincidencia de DISTRITO (base: 310 distritos de interrupciones):** clima 0 % (crudo y normalizado);
peligros 0 % crudo → 90,97 % normalizado; DATASS 62,58 %.

**Limpieza y panel corregidos**

- Interrupciones: 45 671 → 45 000 sin filas idénticas (−671) → 44 729 sin duplicados (ID, depto, distrito)
  (−271) → 44 541 tras rechazar distrito nulo (−188).
- Clima: PP nulo → 0,0 con `PP_IMPUTADO = 1` en 14 440 registros horarios; TEMP nulo → mediana por
  estación y mes en 13 343 registros (0 nulos restantes). Clima distrito-mes: 570 filas (5 distritos × 114 meses).
- Peligros: 37 895 → 37 787 válidas (108 filas mal formadas o con tipo inválido); 1 707 distritos.
- Infraestructura: 1 653 distritos.
- Panel distrito-mes: 323 distritos × 60 meses (2019-06 a 2024-05) = **19 380 filas × 27 columnas**.

| JOIN | izquierda | derecha (claves únicas) | resultado |
|---|---|---|---|
| base × clima (depto-mes) | 19 380 | 342 (342) | 19 380 ✔ |
| × conteo de interrupciones | 19 380 | 6 547 (6 547) | 19 380 ✔ |
| × peligros (broadcast) | 19 380 | 1 707 (1 707) | 19 380 ✔ |
| × infraestructura (broadcast) | 19 380 | 1 653 (1 653) | 19 380 ✔ |

Prevalencia de TARGET = 0,3378 (6 547 distrito-meses con interrupción); 6 180 filas con clima;
11 460 con infraestructura. La Libertad: 13 distritos, 780 filas, 514 interrupciones, 0 filas con clima.

## 3.4 Calidad (`03_calidad.txt`)

| Regla | Resultado real |
|---|---|
| R1 Unicidad | 671 filas idénticas + 271 duplicados (ID, depto, distrito) eliminados; 601 IDs afectan a más de un distrito (se conservan); clima: 0 duplicados en 416 273 |
| R2 Completitud | DISTRITO nulo: 188 (0,41 %); PP: 14 440 (3,47 %); TEMP: 13 343 (3,21 %); HR: 14 121 (3,39 %); ID/fechas: 0 |
| R3 Fechas inválidas | 0 en FECHAINICIO, 0 en FECHAFIN, 0 con fin < inicio, 0 en clima, 0 en peligros (la regla se aplicó; no hubo rechazos) |
| R4 Integridad referencial (323 pares depto-distrito) | Peligros 286 (88,54 %); DATASS 191 (59,13 %); clima por distrito 0 (0,00 %); clima por departamento 103 (31,89 %) |
| R5 PP fuera de 0–500 mm | 0 de 416 273 registros horarios (mín. 0,0; máx. 32,5) y 0 de 17 345 días-estación |
| Cardinalidad | 19 380 filas = 19 380 claves únicas = 323 × 60; suma de N_INTERRUPCIONES = 44 541 = interrupciones limpias |

## 3.5 Almacenamiento processed (`cap06`, `02_procesamiento.txt`)

`hdfs:///datalake_sedalib/processed/master_panel.parquet` (Parquet + Snappy, `partitionBy("ANIO")`):
ANIO=2019 (23 662 B), 2020 (25 417 B), 2021 (27 463 B), 2022 (29 743 B), 2023 (30 270 B),
2024 (24 824 B). Relectura: 19 380 filas. Además: `clima_distrito_mes.parquet` (13 966 B).
El `master_table.parquet` anterior se movió a `/datalake_sedalib/obsoleto/`.

## 3.6 Optimización (`04_optimizacion.txt`)

Consulta: 44 541 interrupciones ⋈ clima depto-mes (342) ⋈ peligros (1 707) ⋈ infraestructura (1 604),
agregada a distrito-mes (6 547 filas). Línea base con `autoBroadcastJoinThreshold = -1`; 1 corrida de
calentamiento + 3 medidas; mediana.

| Modo | Corridas (s) | Mediana | Plan | vs. A |
|---|---|---|---|---|
| A SortMerge, 200 particiones (línea base) | 0,663 / 0,536 / 0,461 | **0,536 s** | 3 × SortMergeJoin | — |
| B Broadcast, 200 particiones | 0,314 / 0,270 / 0,244 | **0,270 s** | 3 × BroadcastHashJoin | −49,6 % (1,99×) |
| C SortMerge, 16 particiones | 0,310 / 0,268 / 0,287 | 0,287 s | 3 × SortMergeJoin | −46,5 % (1,87×) |
| D Broadcast, 16 particiones | 0,180 / 0,349 / 0,178 | **0,180 s** | 3 × BroadcastHashJoin | −66,4 % (2,98×) |

Con este volumen los tiempos están por debajo de 1 s; la diferencia es real pero pequeña en valor
absoluto. Nota: una primera versión con `cache()` daba unos 3 s en ambos modos y no mostraba mejora,
así que se cambió a `localCheckpoint()` para aislar el costo del JOIN.

## 3.7 Escalabilidad (`05_escalabilidad.txt`, `05b_pandas_escalabilidad.txt`)

Mismo pipeline en ambos motores: dropDuplicates → normalizar distrito → dropDuplicates(ID, depto, distrito)
→ fecha → 3 JOIN → groupBy distrito-mes. La réplica se genera en memoria (crossJoin con `range(N)`).
Memoria de Spark = pico del contenedor (cgroup, igual que `docker stats`); spill tomado de
`http://localhost:4040/api/v1`.

| Escala | Filas | Spark (s) | Mem. pico contenedor (MiB) | Spill memoria (MiB) | Spill disco (MiB) | pandas (s) | RSS pandas (MiB) | Estado pandas |
|---|---|---|---|---|---|---|---|---|
| 1X | 45 671 | 3,80 | 1 142 | 0,0 | 0,0 | 0,31 | 166 | OK |
| 10X | 456 710 | 4,27 | 1 665 | 0,0 | 0,0 | 2,81 | 556 | OK |
| 100X | 4 567 100 | 25,87 | 1 955 | 3 737,4 | 388,1 | — | — | **OOM** (SIGKILL a los 32,77 s) |
| 500X | 22 835 500 | **OOM** (66,41 s) | 2 152 | 8 020,0* | 224,7* | no ejecutado | — | — |
| 1000X | 45 671 000 | no ejecutado | — | — | — | no ejecutado | — | — |

\* Última lectura de la API antes del fallo (la Spark UI muere con la JVM).
**PUNTO DE QUIEBRE: pandas = 100X; Spark = 500X.**
Spark UI (corrida de 1X–100X con la UI abierta, `05c_escalabilidad_ui.txt`): stage 62 con
Spill (Memory) 1 843,0 MiB y Spill (Disk) 271,9 MiB.

## 4.1 EDA (`06_analisis_modelo.txt`, `07_graficos_locales.txt`)

**g1 – La Libertad, interrupciones por distrito (jun-2019 a may-2024):** Trujillo 226 (96 imprevistas;
tasa 53,33 % de meses), Moche 74, El Porvenir 51, Salaverry 37, La Esperanza 31, Víctor Larco Herrera 31,
Huanchaco 27, Florencia de Mora 20, Chepén 9, Paiján 4, Simbal 2, Chocope 1, Rázuri 1.

**g2 – Tasa de interrupción por PP_TOTAL mensual (nacional: LIMA, AREQUIPA, TACNA; sin La Libertad):**
0 mm 39,34 % (n = 549); 0–10 mm 50,47 % (2 344); 10–25 mm 58,10 % (840); 25–50 mm 58,26 % (654);
50–100 mm 60,41 % (1 109); >100 mm 57,75 % (684). La tasa sube de 39 % a ~58–60 % cuando hay lluvia,
pero el clima es un proxy departamental de estaciones de sierra.

**g3 – Tasa por cantidad de peligros INGEMMET:** resto del país: 0 → 42,5 %; 1–5 → 31,18 %; 6–20 → 32,42 %;
21–50 → 32,38 %; >50 → 41,75 %. La Libertad: 0 → 22,78 % (3 distritos); 1–5 → 17,14 % (7);
6–20 → 18,33 % (1); 21–50 → 4,17 % (2). No hay una relación monótona.

## 4.2 Modelo base (`06_analisis_modelo.txt`)

Split temporal: entrenamiento 2019-07 a 2023-12; prueba 2024 (ene–may). Solo variables rezagadas
(INT_LAG1, TARGET_LAG1, INT_PREV3) y estáticas (N_PELIGROS, % sistemas >20 años, % sistemas que no operan,
INFRA_DISPONIBLE) + mes (one-hot). Umbral 0,5.

**M1 – todos los distritos (323), sin clima.** Prevalencia: 0,3386 en entrenamiento (17 442 filas) y 0,3876 en prueba (1 615).

| Modelo | AUC-ROC | AUC-PR | TN | FP | FN | TP | Precisión | Recall |
|---|---|---|---|---|---|---|---|---|
| Regresión logística | 0,8021 | **0,7176** | 772 | 217 | 184 | 442 | 0,671 | 0,706 |
| Árbol de decisión | **0,8120** | 0,7142 | 771 | 218 | 174 | 452 | 0,675 | 0,722 |
| Línea base ingenua (tasa histórica del distrito) | 0,7907 | 0,6858 | 863 | 126 | 332 | 294 | 0,700 | 0,470 |

**M2 – 103 distritos con estación SENAMHI en su departamento, + clima rezagado.** Prevalencia: 0,5502 (5 562) / 0,5126 (515).

| Modelo | AUC-ROC | AUC-PR | TN | FP | FN | TP |
|---|---|---|---|---|---|---|
| Regresión logística | 0,7985 | 0,7784 | 157 | 94 | 37 | 227 |
| Árbol de decisión | 0,7785 | 0,7313 | 158 | 93 | 34 | 230 |
| Línea base ingenua | **0,8076** | 0,7725 | 182 | 69 | 63 | 201 |

En M2 los modelos **no superan** a la línea base ingenua en AUC-ROC; el clima rezagado no aporta
(importancia 0 en el árbol).

**Importancia (M1):** árbol: INT_PREV3 0,9774; INT_LAG1 0,0138; N_PELIGROS 0,0042; % no opera 0,0021;
% >20 años 0,0013. Regresión logística (coef. estandarizados): INT_PREV3 +6,1332; TARGET_LAG1 +0,5502;
INFRA_DISPONIBLE −0,1418; N_PELIGROS +0,0949. La historia reciente de interrupciones domina la predicción.

**Top 10 distritos de La Libertad por riesgo (regresión logística M1, probabilidad media en 2024):**
Trujillo 0,9944; Salaverry 0,7047; Víctor Larco Herrera 0,6044; El Porvenir 0,5555; Moche 0,5332;
Chepén 0,4037; Huanchaco 0,3631; Florencia de Mora 0,3453; La Esperanza 0,3283; Paiján 0,1200.

---

## Capturas y leyendas sugeridas

| Archivo | Sección | Leyenda sugerida |
|---|---|---|
| `graficos/arquitectura_v2.png` | 2 / 3.1 | Figura 1. Arquitectura v2 del datalake: fuentes, ingesta batch, HDFS raw/processed, Spark (local; YARN en producción), MLlib y Power BI (planificado), con el contrato entre capas. |
| `cap01_docker_ps.png` | 3.1.1 | Figura 2. Contenedores activos del clúster (namenode, datanode y spark_node con la Spark UI publicada en el puerto 4040). |
| `cap02_hdfs_raw.png` | 3.2 | Figura 3. Zona raw en HDFS con las cuatro carpetas de fuentes y los CSV ingeridos. |
| `cap03_diagnostico.png` | 3.3 | Figura 4. Diagnóstico: 0 % de coincidencia de distrito con el clima y explosión de 45 671 a 3 730 914 filas en el JOIN anterior. |
| `cap04_procesamiento_a.png` | 3.3 | Figura 5. Procesamiento en Spark: deduplicación, imputación de PP/TEMP y agregación de peligros e infraestructura. |
| `cap04_procesamiento_b.png` | 3.3 | Figura 6. Panel distrito-mes: filas antes y después de cada JOIN (19 380 sin explosión) y particiones ANIO en HDFS. |
| `cap05_calidad_a.png` | 3.4 | Figura 7. Reporte de calidad: reglas R1–R5 con sus conteos reales. |
| `cap05_calidad_b.png` | 3.4 | Figura 8. Verificación de cardinalidad del panel (19 380 claves únicas; 44 541 interrupciones conservadas). |
| `cap06_hdfs_processed.png` | 3.5 | Figura 9. Zona processed: master_panel.parquet particionado por ANIO=2019 a ANIO=2024. |
| `cap07_optimizacion_a.png` … `_d.png` | 3.6 | Figura 10. Prueba de rendimiento: corridas, explain() con SortMergeJoin y BroadcastHashJoin, y medianas (0,536 s → 0,180 s). |
| `cap08_sparkui_sql_a.png` | 3.6 | Figura 11. Spark UI (SQL): DAG de la línea base con SortMergeJoin y Exchange (shuffle). |
| `cap08_sparkui_sql_b.png` | 3.6 | Figura 12. Spark UI (SQL): DAG optimizado con BroadcastHashJoin y BroadcastExchange. |
| `cap09_escalabilidad_a.png`, `_b.png` | 3.7 | Figura 13. Escalabilidad 1X–1000X: Spark falla por OOM en 500X y pandas en 100X (punto de quiebre). |
| `cap10_sparkui_stages.png` | 3.7 | Figura 14. Spark UI (Stages): stage 62 de la escala 100X con spill de 1 843,0 MiB en memoria y 271,9 MiB en disco. |
| `cap10_sparkui_stages_lista.png` | 3.7 | Figura 15. Spark UI: etapas por escala (1X, 10X, 100X) con lectura y escritura de shuffle. |
| `graficos/g1_distritos_la_libertad.png` | 4.1 | Figura 16. Interrupciones por distrito en La Libertad (SEDALIB), jun-2019 a may-2024. |
| `graficos/g2_tasa_por_precipitacion.png` | 4.1 | Figura 17. Tasa de interrupción según la precipitación mensual del departamento (Lima, Arequipa, Tacna). |
| `graficos/g3_tasa_por_peligros.png` | 4.1 | Figura 18. Tasa de interrupción según la cantidad de peligros geológicos del distrito: La Libertad frente al resto del país. |
| `cap11_eda.png` | 4.1 | Figura 19. Cifras de los tres gráficos del EDA calculadas en Spark. |
| `cap12_modelo_a.png` … `_c.png` | 4.2 | Figura 20. Modelo base: AUC-ROC, AUC-PR, matriz de confusión, importancia de variables y top 10 de riesgo en La Libertad. |

---

## Fragmentos de código clave

**JOINs corregidos sin explosión** (`procesamiento_spark.py`, extracto: se acortó el mensaje del print)

```python
def join_contado(izq, der, on, how, nombre, bcast=False):
    n_izq = izq.count()
    n_der, n_claves = der.count(), der.select(on).distinct().count()
    res = izq.join(F.broadcast(der) if bcast else der, on, how)
    n_res = res.count()
    print("JOIN %-34s | izq=%7d | der=%6d (claves unicas=%6d) | resultado=%7d"
          % (nombre, n_izq, n_der, n_claves, n_res))
    assert n_res == n_izq, "Explosion de filas en " + nombre
    return res

panel = join_contado(base, clima_depm, ["DEPTO_N", "ANIO", "MES"], "left", "base x clima (depto-mes)")
panel = join_contado(panel, int_dm, KEY_D + ["ANIO", "MES"], "left", "x conteo interrupciones")
panel = join_contado(panel, pel_agg, KEY_D, "left", "x peligros (broadcast)", bcast=True)
panel = join_contado(panel, infra_agg, KEY_D, "left", "x infraestructura (broadcast)", bcast=True)
```

**Imputación** (`procesamiento_spark.py`)

```python
cli = (cli.withColumn("PP_IMPUTADO", F.col("PP").isNull().cast("int"))
          .withColumn("PP", F.coalesce(F.col("PP"), F.lit(0.0))))
med_temp = cli.groupBy("ESTACION", "MES").agg(
    F.expr("percentile_approx(TEMP, 0.5, 10000)").alias("TEMP_MEDIANA"))
cli = (cli.join(F.broadcast(med_temp), ["ESTACION", "MES"], "left")
          .withColumn("TEMP_IMPUTADO", F.col("TEMP").isNull().cast("int"))
          .withColumn("TEMP", F.coalesce(F.col("TEMP"), F.col("TEMP_MEDIANA"))))
```

**partitionBy** (`procesamiento_spark.py`)

```python
panel.repartition("ANIO").write.mode("overwrite").partitionBy("ANIO").parquet(C.PANEL)
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
for st in fs.listStatus(jvm.org.apache.hadoop.fs.Path(C.PANEL)):
    print("  %s %s" % ("DIR " if st.isDirectory() else "FILE", st.getPath().toString()))
```

**Peligros agregados antes del JOIN + broadcast** (`procesamiento_spark.py` / `optimizacion_spark.py`, extracto: la agregación final tiene 4 métricas)

```python
por_tipo = pel.groupBy(*KEY_D, "TIPO").count()
w_tipo = Window.partitionBy(*KEY_D).orderBy(F.desc("count"), "TIPO")
pel_agg = (por_tipo.withColumn("rk", F.row_number().over(w_tipo))
           .groupBy(*KEY_D).agg(F.sum("count").alias("N_PELIGROS"),
                                F.max(F.when(F.col("rk") == 1, F.col("TIPO"))).alias("TIPO_PELIGRO_PRINCIPAL")))

def consulta(bcast):                      # optimizacion_spark.py
    b = F.broadcast if bcast else (lambda d: d)
    return (inte.join(b(clima_depm), ["DEPTO_N", "ANIO", "MES"], "left")
            .join(b(pel_agg), KEY_D, "left").join(b(infra_agg), KEY_D, "left")
            .groupBy(*KEY_D, "ANIO", "MES").agg(F.count("*").alias("N_INT")))
```

**Entrenamiento del modelo** (`analisis_modelo.py`)

```python
train = datos.filter(F.col("ANIO") < anio_test).cache()      # 2019-2023
test = datos.filter(F.col("ANIO") == anio_test).cache()      # 2024
etapas = [Imputer(strategy="median", inputCols=imp_cols, outputCols=[c + "_imp" for c in imp_cols]),
          OneHotEncoder(inputCols=["MES"], outputCols=["MES_oh"], dropLast=True),
          VectorAssembler(inputCols=otras + [c + "_imp" for c in imp_cols] + ["MES_oh"], outputCol="features")]
lr = Pipeline(stages=etapas + [StandardScaler(inputCol="features", outputCol="features_std"),
              LogisticRegression(featuresCol="features_std", labelCol="label", maxIter=100)]).fit(train)
dt = Pipeline(stages=etapas + [DecisionTreeClassifier(featuresCol="features", labelCol="label",
              maxDepth=5, minInstancesPerNode=20, seed=42)]).fit(train)
hist = train.groupBy("DEPTO_N", "DIST_N").agg(F.avg("label").alias("TASA_HIST"))   # línea base ingenua
ingenua = test.join(hist, ["DEPTO_N", "DIST_N"], "left").fillna({"TASA_HIST": pt})
```
