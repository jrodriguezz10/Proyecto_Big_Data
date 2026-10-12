"""5. Escalabilidad: 1X, 10X, 100X, 500X, 1000X del pipeline de interrupciones (Spark).

UNICO script que usa datos replicados. La replica se genera en memoria con
crossJoin(spark.range(N)) y un sufijo en IDINTERRUPCION (no se escribe nada a HDFS).
Pipeline medido (igual que en pandas_escalabilidad.py): dropDuplicates -> normalizar
distrito -> dropDuplicates(ID, depto, distrito) -> fecha -> JOIN clima depto-mes ->
JOIN peligros -> JOIN infraestructura -> groupBy distrito-mes -> write("noop").

Metricas por escala:
  * tiempo (s)
  * memoria pico del contenedor: muestreo cada 0.2 s de /sys/fs/cgroup/memory.current
    menos inactive_file (mismo calculo que 'docker stats')
  * memoryBytesSpilled / diskBytesSpilled: suma de las etapas del job, desde
    http://localhost:4040/api/v1
Si hay OutOfMemoryError se registra y se detiene.

Orden: primero pandas (en el host):
  docker run --rm -m 4g --memory-swap 4g -v "${PWD}:/app" proyecto2-app python /app/src/pandas_escalabilidad.py
luego:
  docker exec spark_node /spark/bin/spark-submit /app/src/escalabilidad_spark.py
Variables opcionales: ESCALAS="1,10,100,500,1000", SPARK_UI_HOLD=<segundos>.
"""
import json
import os
import threading
import time
import urllib.request

from pyspark.sql import functions as F

import comun as C
from comun import norm, titulo

ESCALAS = [int(x) for x in os.environ.get("ESCALAS", "1,10,100,500,1000").split(",")]
API = "http://localhost:4040/api/v1"
SALIDA = "/app/evidencias/escalabilidad_spark%s.json" % ("_parcial" if "ESCALAS" in os.environ else "")
PANDAS = "/app/evidencias/escalabilidad_pandas.json"
KEY_D = ["DEPTO_N", "DIST_N"]

spark = C.spark_session("SEDALIB_Escalabilidad")
sc = spark.sparkContext


class MonitorMemoria(threading.Thread):
    """Memoria del contenedor como la reporta docker stats (usage - inactive_file)."""

    def __init__(self, app_id=None, grupo=None):
        super().__init__(daemon=True)
        self.pico, self.activo = 0, True
        self.app_id, self.grupo, self.spill, self.n = app_id, grupo, None, 0

    @staticmethod
    def leer():
        with open("/sys/fs/cgroup/memory.current") as f:
            uso = int(f.read())
        with open("/sys/fs/cgroup/memory.stat") as f:
            inactive = next(int(l.split()[1]) for l in f if l.startswith("inactive_file "))
        return uso - inactive

    def run(self):
        while self.activo:
            self.pico = max(self.pico, self.leer())
            self.n += 1
            if self.app_id and self.n % 10 == 0:  # cada ~2 s: ultimo spill visto (sirve si la JVM muere por OOM)
                try:
                    self.spill = spill_de_grupo(self.app_id, self.grupo)
                except Exception:  # noqa: BLE001
                    pass
            time.sleep(0.2)


def api(ruta):
    with urllib.request.urlopen(API + ruta, timeout=30) as r:
        return json.loads(r.read().decode())


def spill_de_grupo(app_id, grupo):
    jobs = [j for j in api("/applications/%s/jobs" % app_id) if j.get("jobGroup") == grupo]
    ids = {s for j in jobs for s in j["stageIds"]}
    etapas = [s for s in api("/applications/%s/stages" % app_id) if s["stageId"] in ids]
    mem = sum(s.get("memoryBytesSpilled", 0) for s in etapas)
    disk = sum(s.get("diskBytesSpilled", 0) for s in etapas)
    top = max(etapas, key=lambda s: s.get("diskBytesSpilled", 0), default=None)
    return mem, disk, (top["stageId"] if top and top.get("diskBytesSpilled", 0) else None), len(etapas)


# ------------------------------------------------------------ dimensiones (datos reales)
base = C.leer_interrupciones(spark).cache()
n_base = base.count()
clima_depm = (spark.read.parquet(C.PROCESSED + "clima_distrito_mes.parquet")
              .groupBy("DEPTO_N", "ANIO", "MES").agg(F.avg("PP_TOTAL").alias("PP_TOTAL"))).cache()
pel_agg = (C.leer_peligros(spark).withColumn("DEPTO_N", norm(F.col("NM_DPTO")))
           .withColumn("DIST_N", norm(F.col("NM_DIST"))).filter(F.col("DIST_N").isNotNull())
           .groupBy(*KEY_D).agg(F.count("*").alias("N_PELIGROS"))).cache()
infra_agg = (C.leer_infra(spark, "datass_antiguedad").withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO")))
             .withColumn("DIST_N", norm(F.col("DISTRITO")))
             .groupBy(*KEY_D).agg(F.avg(norm(F.col("VALOR")).contains("MAS DE 20").cast("double")).alias("PCT_MAS_20"))).cache()
for d in (clima_depm, pel_agg, infra_agg):
    d.count()


def pipeline(n):
    rep = base.crossJoin(spark.range(n).withColumnRenamed("id", "REP")) \
        .withColumn("IDINTERRUPCION", F.concat_ws("_", "IDINTERRUPCION", F.col("REP").cast("string"))).drop("REP")
    df = (rep.dropDuplicates()
          .withColumn("DEPTO_N", norm(F.col("DEPARTAMENTO"))).withColumn("DIST_N", norm(F.col("DISTRITO")))
          .dropDuplicates(["IDINTERRUPCION", "DEPTO_N", "DIST_N"])
          .withColumn("F_INI", F.to_date("FECHAINICIO", "yyyyMMdd"))
          .filter(F.col("F_INI").isNotNull() & F.col("DIST_N").isNotNull())
          .withColumn("ANIO", F.year("F_INI")).withColumn("MES", F.month("F_INI")))
    return (df.join(clima_depm, ["DEPTO_N", "ANIO", "MES"], "left")
            .join(pel_agg, KEY_D, "left").join(infra_agg, KEY_D, "left")
            .groupBy(*KEY_D, "ANIO", "MES")
            .agg(F.count("*").alias("N_INT"), F.avg("PP_TOTAL").alias("PP_TOTAL"),
                 F.max("N_PELIGROS").alias("N_PELIGROS"), F.max("PCT_MAS_20").alias("PCT_MAS_20")))


titulo("5. PRUEBA DE ESCALABILIDAD - SPARK (modo local)")
print("cores=%d | spark.driver.memory=%s | shuffle.partitions=%s | filas base=%d"
      % (sc.defaultParallelism, spark.conf.get("spark.driver.memory", "1g (por defecto)"),
         spark.conf.get("spark.sql.shuffle.partitions"), n_base))
app_id = sc.applicationId
CORES = sc.defaultParallelism
resultados = []
for n in ESCALAS:
    grupo = "escala_%dX" % n
    sc.setJobGroup(grupo, "Escalabilidad %dX" % n)
    mon = MonitorMemoria(app_id, grupo)
    mem_inicio = mon.leer()
    mon.start()
    t0 = time.perf_counter()
    estado = "OK"
    try:
        pipeline(n).write.format("noop").mode("overwrite").save()
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "OutOfMemoryError" in msg or "Java heap space" in msg or "GC overhead" in msg:
            estado = "OOM"
        elif sc._jsc.sc().isStopped():
            # el UncaughtExceptionHandler detuvo el SparkContext; la causa (OutOfMemoryError) queda en el log
            estado = "OOM (SparkContext detenido; ver java.lang.OutOfMemoryError en el log)"
        else:
            estado = "ERROR: " + msg.splitlines()[0][:120]
    t = time.perf_counter() - t0
    mon.activo = False
    mon.join()
    r = {"escala": n, "filas": n_base * n, "tiempo_s": round(t, 2), "estado": estado,
         "mem_inicio_mib": round(mem_inicio / 2 ** 20), "mem_pico_mib": round(mon.pico / 2 ** 20)}
    try:
        m, d, top, n_et = spill_de_grupo(app_id, grupo)
        r.update(spill_mem_mib=round(m / 2 ** 20, 1), spill_disk_mib=round(d / 2 ** 20, 1), etapa_max_spill=top, etapas=n_et)
    except Exception as e:  # noqa: BLE001
        if mon.spill:  # la UI murio con la JVM: se usa la ultima lectura de la API durante la corrida
            m, d, top, n_et = mon.spill
            r.update(spill_mem_mib=round(m / 2 ** 20, 1), spill_disk_mib=round(d / 2 ** 20, 1), etapa_max_spill=top,
                     etapas=n_et, nota_spill="ultima lectura de la API antes del fallo")
        else:
            r.update(spill_mem_mib=None, spill_disk_mib=None, etapa_max_spill=None, etapas=None)
        print("   (API REST no disponible al final: %s)" % e)
    resultados.append(r)
    print("-> %5dX | filas=%11d | %8.2f s | mem pico contenedor=%6d MiB | spill mem=%9s MiB | spill disco=%8s MiB | %s"
          % (n, r["filas"], t, r["mem_pico_mib"], r["spill_mem_mib"], r["spill_disk_mib"], estado), flush=True)
    if estado != "OK":
        print("   Se detiene la prueba por %s en %dX" % (estado, n))
        break

with open(SALIDA, "w") as f:
    json.dump(resultados, f, indent=1)

# ------------------------------------------------------------ tabla comparativa
pandas = {}
if os.path.exists(PANDAS):
    with open(PANDAS) as f:
        pandas = {r["escala"]: r for r in json.load(f)["resultados"]}
    with open(PANDAS) as f:
        lim_pd = json.load(f).get("limite_memoria_contenedor", "?")
else:
    lim_pd = "?"
    print("\n(No existe %s: ejecute antes pandas_escalabilidad.py)" % PANDAS)

titulo("TABLA FINAL: SPARK vs PANDAS")
print("Spark: modo local[%d], heap del driver 1 GiB | pandas: contenedor con limite %s" % (CORES, lim_pd))
cab = "%-6s %12s | %9s %10s %11s %11s | %9s %10s %s"
print(cab % ("escala", "filas", "Spark s", "mem MiB", "spill mem", "spill disk", "pandas s", "RSS MiB", "pandas estado"))
print("-" * 112)
quiebre_pd = quiebre_sp = None
for n in ESCALAS:
    s = next((r for r in resultados if r["escala"] == n), None)
    p = pandas.get(n)
    if s and s["estado"] != "OK" and quiebre_sp is None:
        quiebre_sp = n
    if p and p["estado"] != "OK" and quiebre_pd is None:
        quiebre_pd = n
    print(cab % ("%dX" % n, "{:,}".format(n_base * n),
                 ("%.2f" % s["tiempo_s"]) if s and s["estado"] == "OK" else ("OOM" if s and s["estado"].startswith("OOM") else (s["estado"][:9] if s else "-")),
                 s["mem_pico_mib"] if s else "-", s["spill_mem_mib"] if s else "-", s["spill_disk_mib"] if s else "-",
                 ("%.2f" % p["tiempo_s"]) if p and p["estado"] == "OK" else "-",
                 p["rss_pico_mib"] if p and p.get("rss_pico_mib") else "-",
                 p["estado"] if p else "no ejecutado (se detuvo antes)"))
print("-" * 112)
print("PUNTO DE QUIEBRE pandas : %s" % ("%dX" % quiebre_pd if quiebre_pd else "no se alcanzo en las escalas probadas"))
print("PUNTO DE QUIEBRE Spark  : %s" % ("%dX" % quiebre_sp if quiebre_sp else "no se alcanzo (completo hasta %dX)" % resultados[-1]["escala"]))
top = max(resultados, key=lambda r: r.get("spill_disk_mib") or 0)
if top.get("etapa_max_spill") is not None:
    print("Etapa con mas spill a disco: stage %s (escala %dX) -> http://localhost:4040/stages/stage/?id=%s&attempt=0"
          % (top["etapa_max_spill"], top["escala"], top["etapa_max_spill"]))

hold = int(os.environ.get("SPARK_UI_HOLD", "0"))
if hold:
    print("\nSpark UI disponible en http://localhost:4040/stages/ durante %d s" % hold, flush=True)
    fin = time.time() + hold
    while time.time() < fin and not os.path.exists("/app/evidencias/.liberar_ui"):
        time.sleep(1)
spark.stop()
