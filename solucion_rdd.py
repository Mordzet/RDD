import os
import sys
import json
import time
import shutil

os.environ["JAVA_HOME"] = r"C:\Users\saidg\.jdks\temurin-17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("RDD_Practicas_y_Proyecto") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

outputs = {}

texto = ["hola mundo", "hola spark", "mundo spark"]
rdd_texto = sc.parallelize(texto)
conteo_palabras = rdd_texto.flatMap(lambda linea: linea.split(" ")) \
                           .map(lambda palabra: (palabra, 1)) \
                           .reduceByKey(lambda a, b: a + b)
res_ej1 = conteo_palabras.collect()
out_ej1 = (
    f"Texto original: {texto}\n"
    f"Conteo de palabras: {res_ej1}\n"
    + "\n".join([f"  - '{palabra}': {cantidad}" for palabra, cantidad in res_ej1])
)
print("\n--- EJERCICIO 1 ---")
print(out_ej1)
outputs["ej1"] = out_ej1

ventas_ej2 = [("Madrid", 100), ("Barcelona", 200), ("Madrid", 50)]
rdd_ventas2 = sc.parallelize(ventas_ej2)

ventas_por_ciudad = rdd_ventas2.reduceByKey(lambda a, b: a + b)
ventas_filtradas = ventas_por_ciudad.filter(lambda x: x[1] > 100)
ventas_ordenadas = ventas_filtradas.sortBy(lambda x: x[1], ascending=False)

out_ej2 = (
    f"1. Suma de ventas por ciudad: {ventas_por_ciudad.collect()}\n"
    f"2. Ciudades con ventas > 100: {ventas_filtradas.collect()}\n"
    f"3. Ordenadas de mayor a menor: {ventas_ordenadas.collect()}"
)
print("\n--- EJERCICIO 2 ---")
print(out_ej2)
outputs["ej2"] = out_ej2

precios_usd = [10, 25, 40, 5, 100, 15]
rdd_precios = sc.parallelize(precios_usd)

precios_filtrados = rdd_precios.filter(lambda x: x >= 20)
precios_eur = precios_filtrados.map(lambda x: round(x * 0.9, 2))

out_ej3 = (
    f"Precios en USD originales: {precios_usd}\n"
    f"Precios filtrados (>= 20 USD): {precios_filtrados.collect()}\n"
    f"Precios convertidos a EUR (x0.9): {precios_eur.collect()}"
)
print("\n--- EJERCICIO 3 ---")
print(out_ej3)
outputs["ej3"] = out_ej3

csv_lineas = ["Leche,Huevos,Pan", "Manzanas,Peras", "Leche,Chocolate"]
rdd_csv = sc.parallelize(csv_lineas)

productos_aplanados = rdd_csv.flatMap(lambda linea: linea.split(","))
productos_unicos = productos_aplanados.distinct()
total_unicos = productos_unicos.count()

out_ej4 = (
    f"Todos los productos (flatMap): {productos_aplanados.collect()}\n"
    f"Productos únicos (distinct): {productos_unicos.collect()}\n"
    f"Total de productos únicos (count): {total_unicos}"
)
print("\n--- EJERCICIO 4 ---")
print(out_ej4)
outputs["ej4"] = out_ej4

rdd_logs = sc.textFile("logs.txt")
peticiones_por_ip = rdd_logs.map(lambda linea: (linea.split(",")[0], 1)) \
                            .reduceByKey(lambda a, b: a + b)
ips_frecuentes = peticiones_por_ip.filter(lambda x: x[1] > 1)

output_dir_logs = "output_logs_ip"
if os.path.exists(output_dir_logs):
    shutil.rmtree(output_dir_logs)

try:
    ips_frecuentes.saveAsTextFile(output_dir_logs)
    metodo_guardado = f"saveAsTextFile('{output_dir_logs}')"
except Exception:
    os.makedirs(output_dir_logs, exist_ok=True)
    with open(os.path.join(output_dir_logs, "part-00000"), "w", encoding="utf-8") as f:
        for item in ips_frecuentes.collect():
            f.write(f"{item}\n")
    metodo_guardado = f"Archivo guardado en '{output_dir_logs}/part-00000' y 'resultado_logs_ips.txt'"

with open("resultado_logs_ips.txt", "w", encoding="utf-8") as f:
    for ip, cant in ips_frecuentes.collect():
        f.write(f"{ip},{cant}\n")

out_ej5 = (
    f"1. Registros cargados de logs.txt ({rdd_logs.count()} líneas): {rdd_logs.collect()}\n"
    f"2. Peticiones por cada IP: {peticiones_por_ip.collect()}\n"
    f"3. IPs con más de 1 petición: {ips_frecuentes.collect()}\n"
    f"4. Resultado guardado exitosamente en disco ({metodo_guardado})."
)
print("\n--- EJERCICIO 5 ---")
print(out_ej5)
outputs["ej5"] = out_ej5

ventas_ej6 = [("Madrid", 100), ("Barcelona", 200), ("Madrid", 50), ("Valencia", 50)]
rdd_ventas6 = sc.parallelize(ventas_ej6)

pipeline_mayores_60 = rdd_ventas6.filter(lambda x: x[1] >= 60) \
                                 .reduceByKey(lambda a, b: a + b) \
                                 .sortBy(lambda x: x[1], ascending=False)

out_ej6 = (
    f"Ventas originales: {ventas_ej6}\n"
    f"1. Tras filtrar (descartar) ventas menores de 60 (venta >= 60): {rdd_ventas6.filter(lambda x: x[1] >= 60).collect()}\n"
    f"2 y 3. Suma por ciudad ordenada por facturación (mayor a menor): {pipeline_mayores_60.collect()}"
)
print("\n--- EJERCICIO 6 ---")
print(out_ej6)
outputs["ej6"] = out_ej6

frases_ej7 = ["Hola mundo", "Apache Spark es guay"]
rdd_frases7 = sc.parallelize(frases_ej7)

res_map = rdd_frases7.map(lambda frase: frase.split(" ")).collect()
res_flatmap = rdd_frases7.flatMap(lambda frase: frase.split(" ")).collect()

out_ej7 = (
    f"1. Resultado con map(split(' ')):\n   {res_map}\n"
    f"   -> Estructura: Lista de listas (listas anidadas, 1 elemento de salida por cada frase de entrada).\n\n"
    f"2. Resultado con flatMap(split(' ')):\n   {res_flatmap}\n"
    f"   -> Diferencia: Aplana las listas anidadas en una única lista simple de palabras (1 frase -> N palabras)."
)
print("\n--- EJERCICIO 7 ---")
print(out_ej7)
outputs["ej7"] = out_ej7

rdd_reutilizado = sc.parallelize(range(1, 100001), 2).map(lambda x: x * 2)
rdd_reutilizado.cache()

t0 = time.time()
c1 = rdd_reutilizado.count()
t1 = time.time()
c2 = rdd_reutilizado.reduce(lambda a, b: a + b)
c3 = rdd_reutilizado.filter(lambda x: x % 1000 == 0).count()
c4 = rdd_reutilizado.max()
c5 = rdd_reutilizado.min()
t2 = time.time()

nivel_storage = rdd_reutilizado.getStorageLevel()
rdd_reutilizado.unpersist()

out_ej8 = (
    "Respuesta: Debemos persistir el RDD en memoria usando `rdd.cache()` o `rdd.persist(StorageLevel.MEMORY_AND_DISK)`\n"
    "antes de ejecutar las 5 acciones, y liberar la memoria al final con `rdd.unpersist()`.\n"
    "Sin cache(), Spark recalcularía todo el linaje (DAG) 5 veces desde cero.\n\n"
    f"Demostración práctica ejecutada:\n"
    f"  - Nivel de persistencia activo: {nivel_storage}\n"
    f"  - Cálculo 1 (count - materializa cache): {c1} elementos ({t1 - t0:.4f} s)\n"
    f"  - Cálculo 2 (suma total desde cache): {c2}\n"
    f"  - Cálculo 3 (múltiplos de 1000 desde cache): {c3}\n"
    f"  - Cálculo 4 (valor máximo desde cache): {c4}\n"
    f"  - Cálculo 5 (valor mínimo desde cache): {c5}\n"
    f"  - Tiempo total de los cálculos 2 al 5 leyendo desde RAM: {t2 - t1:.4f} s"
)
print("\n--- EJERCICIO 8 ---")
print(out_ej8)
outputs["ej8"] = out_ej8

def parsear_linea_nota(linea):
    try:
        linea = linea.strip()
        if not linea or "," not in linea:
            return None
        partes = linea.split(",")
        nombre = partes[0].strip()
        nota = float(partes[1].strip())
        return (nombre, nota)
    except Exception:
        return None

rdd_mates_raw = sc.textFile("notas_mates.txt")
rdd_fisica_raw = sc.textFile("notas_fisica.txt")
rdd_ingles_raw = sc.textFile("notas_ingles.txt")

rdd_mates = rdd_mates_raw.map(parsear_linea_nota).filter(lambda x: x is not None).cache()
rdd_fisica = rdd_fisica_raw.map(parsear_linea_nota).filter(lambda x: x is not None).cache()
rdd_ingles = rdd_ingles_raw.map(parsear_linea_nota).filter(lambda x: x is not None).cache()

rdd_todas_notas = rdd_mates.union(rdd_fisica).union(rdd_ingles).cache()

promedio_por_alumno = rdd_todas_notas.mapValues(lambda nota: (nota, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: round(x[0] / x[1], 2)) \
    .sortBy(lambda x: x[1], ascending=False)

nota_max_alumno = rdd_todas_notas.reduceByKey(lambda a, b: a if a > b else b)
nota_min_alumno = rdd_todas_notas.reduceByKey(lambda a, b: a if a < b else b)

aprobados_promedio = promedio_por_alumno.filter(lambda x: x[1] >= 5.0)
suspensos_promedio = promedio_por_alumno.filter(lambda x: x[1] < 5.0)

alumnos_mates = rdd_mates.map(lambda x: x[0])
alumnos_fisica = rdd_fisica.map(lambda x: x[0])
alumnos_ingles = rdd_ingles.map(lambda x: x[0])

todos_alumnos_unicos = alumnos_mates.union(alumnos_fisica).union(alumnos_ingles).distinct()
alumnos_en_las_tres = alumnos_mates.intersection(alumnos_fisica).intersection(alumnos_ingles)
alumnos_solo_mates = alumnos_mates.subtract(alumnos_fisica).subtract(alumnos_ingles)

total_calificaciones = rdd_todas_notas.count()
suma_global = rdd_todas_notas.map(lambda x: x[1]).reduce(lambda a, b: a + b)
media_global = round(suma_global / total_calificaciones, 2)
mejor_calificacion = rdd_todas_notas.reduce(lambda a, b: a if a[1] > b[1] else b)
peor_calificacion = rdd_todas_notas.reduce(lambda a, b: a if a[1] < b[1] else b)

out_proyecto = (
    "============================================================\n"
    "       INFORME ESTADÍSTICO DE CALIFICACIONES (RDDs)         \n"
    "============================================================\n"
    f"1. DATOS CARGADOS Y LIMPIOS POR ASIGNATURA:\n"
    f"   - Matemáticas: {rdd_mates.collect()}\n"
    f"   - Física:      {rdd_fisica.collect()}\n"
    f"   - Inglés:      {rdd_ingles.collect()}\n"
    f"   - Total registros unificados (union): {rdd_todas_notas.collect()}\n\n"
    f"2. ESTADÍSTICAS POR ALUMNO (reduceByKey / mapValues):\n"
    f"   - Nota media por alumno (ordenado desc): {promedio_por_alumno.collect()}\n"
    f"   - Nota máxima por alumno:                {nota_max_alumno.collect()}\n"
    f"   - Nota mínima por alumno:                {nota_min_alumno.collect()}\n\n"
    f"3. CLASIFICACIÓN DE RENDIMIENTO (filter sobre nota media >= 5.0):\n"
    f"   - Alumnos aprobados (media >= 5.0):      {aprobados_promedio.collect()}\n"
    f"   - Alumnos suspensos (media < 5.0):       {suspensos_promedio.collect()}\n\n"
    f"4. ANÁLISIS DE MATRÍCULA (Operaciones de Conjuntos):\n"
    f"   - Lista única de alumnos (distinct):               {todos_alumnos_unicos.collect()}\n"
    f"   - Alumnos evaluados en las 3 asignaturas (intersection): {alumnos_en_las_tres.collect()}\n"
    f"   - Alumnos evaluados SOLO en Matemáticas (subtract):      {alumnos_solo_mates.collect()}\n\n"
    f"5. MÉTRICAS GLOBALES DEL INSTITUTO:\n"
    f"   - Total de exámenes registrados: {total_calificaciones}\n"
    f"   - Promedio general del instituto: {media_global}\n"
    f"   - Calificación más alta: {mejor_calificacion[0]} ({mejor_calificacion[1]})\n"
    f"   - Calificación más baja: {peor_calificacion[0]} ({peor_calificacion[1]})\n"
    "============================================================"
)
print("\n--- PROYECTO PRÁCTICO ---")
print(out_proyecto)
outputs["proyecto"] = out_proyecto

with open("outputs_ejecucion.json", "w", encoding="utf-8") as f:
    json.dump(outputs, f, ensure_ascii=False, indent=2)

spark.stop()
