# Dashboard SEDALIB: riesgo de interrupción por distrito y mes

Este dashboard en Streamlit lee la capa gold del datalake. Esa capa se escribe en `s3://<bucket>/curated/gold/`, en formato Parquet, y está registrada en Glue en la base `sedalib_gold`. El dashboard no lee de S3: usa los CSV de `dashboard/data/`, que están en el repositorio. Por eso no necesita credenciales de AWS ni para correr en local ni en la nube.

## Contenido

| Sección | Qué muestra | Fuente |
|---|---|---|
| Filtros | Departamento (incluye la opción NACIONAL) y mes | `dim_tiempo` |
| 4 tarjetas de KPI | Distritos en alerta, tasa de interrupción real, precisión y recall de las alertas | `gold_kpi_mensual` |
| Ranking de La Libertad | Probabilidad por distrito en el mes elegido; los distritos en alerta aparecen en rojo y con la etiqueta ALERTA | `gold_riesgo_distrito_mes` |
| Tendencia mensual | Distritos en alerta frente a distritos con interrupción real | `gold_kpi_mensual` |
| Modelo frente a tasa histórica | AUC-PR, AUC-ROC, precisión y recall en el holdout 2024 | `gold_comparacion_modelos` |

## Ejecutar en local

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

El dashboard queda en http://localhost:8501.

## Actualizar los datos

Este paso solo hace falta si se vuelve a generar la capa gold. Requiere credenciales de AWS en la máquina.

```bash
python aws/05_gold_athena.py --exportar    # descarga curated/gold/ a dashboard/data/*.csv
```

Después hay que hacer commit y push de `dashboard/data/` para que la versión publicada use los datos nuevos.

## Publicar gratis en Streamlit Community Cloud

1. Comprueba que el repositorio de GitHub tenga en `main` los archivos `dashboard/app.py`, `dashboard/requirements.txt` y `dashboard/data/*.csv`. Si el repositorio es privado, Community Cloud pedirá permiso para leerlo.
2. Entra en https://share.streamlit.io e inicia sesión con tu cuenta de GitHub.
3. Pulsa **Create app**, elige **Deploy a public app from GitHub** y completa los campos:
   - **Repository:** el repositorio del proyecto.
   - **Branch:** `main`.
   - **Main file path:** `dashboard/app.py`.
   - **App URL:** el subdominio que prefieras, por ejemplo `sedalib-riesgo`.
4. En **Advanced settings**, elige Python 3.12. No agregues *secrets*, porque el dashboard no usa AWS.
5. Pulsa **Deploy**. Community Cloud instalará `dashboard/requirements.txt`, que es el archivo que está junto a `app.py`, y publicará la app en `https://<subdominio>.streamlit.app`.
6. Cada push a `main` vuelve a desplegar la app automáticamente.

Hay que tener en cuenta dos comportamientos del plan gratuito:

- Una app que no recibe visitas durante unos días se suspende. Se reactiva con el botón **Wake up**.
- Los CSV publicados no tienen datos personales. Son agregados por distrito y mes: probabilidad, alerta e indicador de interrupción.
