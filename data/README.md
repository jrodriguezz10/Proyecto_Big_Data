# Datos de entrada (no incluidos en el repositorio)

Los CSV/XLSX originales y el Parquet procesado **no se suben**. Para reproducir el proyecto, coloca
los archivos en `data/raw/` con esta estructura y cárgalos a HDFS (ver el README principal):

| Ruta local | Fuente | Contenido | Filas leídas |
|---|---|---|---|
| `data/raw/operaciones/sunass_interrupciones.csv` | SUNASS | Interrupciones del servicio por EPS (2019–2024) | 45 671 |
| `data/raw/operaciones/sunass_anf.csv` | SUNASS | % de agua no facturada por EPS (sin año ni volumen) | 16 |
| `data/raw/operaciones/sunass_hidrojets.csv` | SUNASS | Actividades de hidrojet (FECHA_CORTE 2025-06-30) | 9 690 |
| `data/raw/clima/senamhi_clima.csv` | SENAMHI | Registros horarios de 5 estaciones, 2015-01-01 a 2024-06-30 | 416 273 |
| `data/raw/geologia/ingemmet_peligros.csv` | INGEMMET | Inventario de peligros geológicos (189 columnas, `;`, ISO-8859-1) | 37 895 |
| `data/raw/infraestructura/datass_antiguedad.xlsx` | DATASS (MVCS) | Antigüedad de sistemas de agua (≤20 / >20 años) | 21 171 |
| `data/raw/infraestructura/datass_estado.xlsx` | DATASS (MVCS) | Estado operativo de sistemas de agua | 11 444 |

`src/ingesta_infraestructura.py` convierte los dos XLSX a CSV (`;`, UTF-8) antes de subirlos a HDFS.

## Enlaces oficiales (portales de origen)

- SUNASS: https://www.sunass.gob.pe/ — conjuntos publicados en la Plataforma Nacional de Datos Abiertos: https://www.datosabiertos.gob.pe/
- SENAMHI: https://www.senamhi.gob.pe/ y https://www.datosabiertos.gob.pe/
- INGEMMET (GEOCATMIN): https://geocatmin.ingemmet.gob.pe/
- DATASS – Ministerio de Vivienda, Construcción y Saneamiento: https://datass.vivienda.gob.pe/

**Pendiente de completar por el equipo:** el enlace exacto de cada conjunto de datos y su licencia
(verificar en la ficha del dataset; los conjuntos de datosabiertos.gob.pe indican su licencia en la ficha).

## Fecha de descarga

No se registró la fecha exacta. Fechas de modificación de los archivos locales: 12/09/2026
(INGEMMET), 23/09/2026 (interrupciones e hidrojets), 24/09/2026 (ANF, SENAMHI y DATASS).
Fechas de corte declaradas en los propios archivos: SENAMHI 2024-06-30; hidrojets 2025-06-30.
