"""Punto de entrada de los jobs en EMR Serverless (se sube a s3://<bucket>/scripts/).

Ejecuta SIN CAMBIOS un script de src/ (llegado por --py-files junto con comun.py) y, al terminar,
copia los archivos que el script haya escrito en SEDALIB_EVID (carpeta local del driver) a
SEDALIB_EVID_DESTINO (s3://<bucket>/curated/...). Asi el mismo codigo corre en Docker y en AWS:
solo cambian las variables de entorno (ver src/comun.py).

    entryPoint: s3://<bucket>/scripts/emr_lanzador.py   entryPointArguments: [modelo_definitivo.py]
"""
import importlib.util
import os
import runpy
import sys
import time

from pyspark.sql import SparkSession

script = sys.argv[1]
modulo = os.path.splitext(os.path.basename(script))[0]
spec = importlib.util.find_spec(modulo)
if spec is None or not spec.origin:
    sys.exit("No se encontro %s en --py-files" % script)

evid = os.environ.get("SEDALIB_EVID")
destino = os.environ.get("SEDALIB_EVID_DESTINO")
if evid:
    os.makedirs(evid, exist_ok=True)

# Los scripts terminan con spark.stop(); se pospone hasta copiar las salidas locales a S3.
_stop = SparkSession.stop
SparkSession.stop = lambda self: None
sys.argv = [spec.origin]
t0 = time.perf_counter()
try:
    runpy.run_path(spec.origin, run_name="__main__")
finally:
    print("[lanzador] tiempo del script %s: %.2f s" % (script, time.perf_counter() - t0), flush=True)
    SparkSession.stop = _stop
    spark = SparkSession.getActiveSession()
    if spark is not None:
        if evid and destino and os.listdir(evid):
            jvm = spark.sparkContext._jvm
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(destino), conf)
            for nombre in sorted(os.listdir(evid)):
                fs.copyFromLocalFile(False, True, jvm.org.apache.hadoop.fs.Path(os.path.join(evid, nombre)),
                                     jvm.org.apache.hadoop.fs.Path(destino.rstrip("/") + "/" + nombre))
                print("[lanzador] copiado %s -> %s%s" % (nombre, destino.rstrip("/") + "/", nombre))
        spark.stop()
