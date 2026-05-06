# =====================================================================
# SCRIPT MAESTRO DE EXPLORACIÓN (EDA) - CAPA BRONZE (VERSIÓN LOGGER)
# Objetivo: Perfilado absoluto de datos con trazabilidad profesional
# =====================================================================

# Importamos la librería de auditoría nativa de Python
import logging
# Importamos todas las funciones necesarias, incluyendo regexp_extract para buscar patrones complejos
from pyspark.sql.functions import col, count, when, isnull, countDistinct, trim, length, regexp_extract

# ---------------------------------------------------------------------
# CONFIGURACIÓN DE AUDITORÍA (LOGS)
# ---------------------------------------------------------------------
# Configuramos el sistema de reportes para registrar los eventos del EDA
logging.basicConfig(
    # Filtramos para ver solo mensajes de información y errores
    level=logging.INFO,
    # Definimos el formato visual: Tiempo exacto - Nivel de alerta - Mensaje
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Instanciamos el reporteador exclusivo para este script
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# FASE 0: EXTRACCIÓN Y MUESTREO ESTADÍSTICO (SAMPLING)
# ---------------------------------------------------------------------
logger.info("--- 0. CARGA DE DATOS OPTIMIZADA ---")

# spark.read.table() va al catálogo y busca nuestra tabla Parquet
# .sample() le dice a Spark que NO lea toda la tabla para ahorrar dinero
# withReplacement=False asegura que no escoja la misma fila dos veces al azar
# fraction=0.10 extrae solo el 10% del total de los datos
# seed=42 es una semilla aleatoria fija; garantiza que siempre saque el mismo 10%
df_bronze_analisis = spark.read.table("default.bronze_patients").sample(withReplacement=False, fraction=0.10, seed=42)

logger.info("Datos cargados exitosamente (Muestra del 10%).")

# ---------------------------------------------------------------------
# FASE 1: VOLUMETRÍA Y DUPLICADOS
# ---------------------------------------------------------------------
logger.info("--- 1. ANÁLISIS DE VOLUMETRÍA ---")

# count() cuenta físicamente cuántas filas trajo nuestra muestra
total_filas = df_bronze_analisis.count()
logger.info(f"Total de filas en la muestra: {total_filas}")

# agg() ejecuta una agregación matemática sobre todo el DataFrame
# countDistinct("Id") cuenta cuántos 'Id' únicos existen realmente
# collect()[0][0] saca ese número de la tabla para guardarlo en la variable 'total_unicos'
total_unicos = df_bronze_analisis.agg(countDistinct("Id")).collect()[0][0]

# Comparamos lógicamente para ver si hay filas repetidas
if total_filas > total_unicos:
    logger.warning(f"¡ALERTA! Encontramos {total_filas - total_unicos} registros duplicados.")
else:
    logger.info("No hay pacientes duplicados en la muestra.")

# ---------------------------------------------------------------------
# FASE 2: MAPA DE VALORES NULOS (VACÍOS)
# ---------------------------------------------------------------------
logger.info("--- 2. MAPA DE VALORES NULOS ---")
logger.info("Generando visualización de valores nulos en Databricks...")

# select() crea una nueva proyección de columnas
# for c in df_bronze_analisis.columns: recorre cada nombre de columna dinámicamente
# when(isnull(c), c) evalúa: "Si esta celda está vacía, tómala en cuenta"
# count() suma todas las celdas vacías encontradas
# alias(c) le devuelve el nombre original a la columna para no perdernos en el resultado
df_nulos = df_bronze_analisis.select([count(when(isnull(c), c)).alias(c) for c in df_bronze_analisis.columns])

# display() le pide a Databricks que dibuje una tabla gráfica con la sumatoria de nulos
display(df_nulos)

# ---------------------------------------------------------------------
# FASE 3: RESUMEN ESTADÍSTICO MATEMÁTICO
# ---------------------------------------------------------------------
logger.info("--- 3. ESTADÍSTICA MATEMÁTICA ---")
logger.info("Calculando matriz de distribución estadística...")

# summary() escanea las columnas numéricas y de fecha
# Calcula automáticamente la media, mínimos y máximos para buscar valores atípicos (outliers)
display(df_bronze_analisis.summary())

# ---------------------------------------------------------------------
# FASE 4: ANÁLISIS DE TEXTOS Y ESPACIOS OCULTOS
# ---------------------------------------------------------------------
logger.info("--- 4. ANÁLISIS DE ESPACIOS OCULTOS ---")

# filter() busca qué filas cumplen una condición de error
# col("CITY") extrae el texto exacto que vino de origen
# != evalúa si ese texto es diferente a su versión "limpia"
# trim(col("CITY")) simula cortar los espacios en blanco a la izquierda o derecha
df_espacios = df_bronze_analisis.filter(col("CITY") != trim(col("CITY")))

# count() suma cuántas ciudades tienen espacios basura
logger.info(f"Registros con espacios ocultos en la columna CITY: {df_espacios.count()}")

# ---------------------------------------------------------------------
# FASE 5: CONSISTENCIA CATEGÓRICA
# ---------------------------------------------------------------------
logger.info("--- 5. CONSISTENCIA CATEGÓRICA ---")

logger.info("Valores únicos detectados en GENDER:")
# select("GENDER") aísla esa única columna
# distinct() elimina repeticiones, creando un diccionario de lo que realmente existe
# show() imprime la tablita resultante en la consola (mantenemos show para ver la estructura)
df_bronze_analisis.select("GENDER").distinct().show()

logger.info("Valores únicos detectados en MARITAL:")
# Repetimos la lógica exacta para buscar errores tipográficos en el estado civil
df_bronze_analisis.select("MARITAL").distinct().show()

# ---------------------------------------------------------------------
# FASE 6: LÓGICA DE NEGOCIO CRUZADA
# ---------------------------------------------------------------------
logger.info("--- 6. LÓGICA DE NEGOCIO CRUZADA ---")

# filter() busca anomalías lógicas comparando dos columnas
# Evaluamos si la fecha de muerte (DEATHDATE) es menor (<) a la de nacimiento (BIRTHDATE)
df_fechas_ilogicas = df_bronze_analisis.filter(col("DEATHDATE") < col("BIRTHDATE"))

# count() suma cuántos registros son biológicamente imposibles
logger.info(f"Pacientes con defunción previa al nacimiento: {df_fechas_ilogicas.count()}")

# ---------------------------------------------------------------------
# FASE 7: CALIDAD ESTRUCTURAL (SUPERVISOR CHECKS)
# ---------------------------------------------------------------------
logger.info("--- 7. CALIDAD ESTRUCTURAL ---")

# length(col("Id")) mide la cantidad de letras/números del ID
# != 36 busca los que no cumplen con el estándar de 36 caracteres de un UUID
df_id_roto = df_bronze_analisis.filter(length(col("Id")) != 36)
logger.info(f"IDs con longitud incorrecta (posible corrupción): {df_id_roto.count()}")

# Buscamos filas fantasma encadenando validaciones de nulos con el operador '&' (AND)
df_filas_fantasma = df_bronze_analisis.filter(isnull(col("Id")) & isnull(col("BIRTHDATE")) & isnull(col("GENDER")))
logger.info(f"Filas completamente vacías (fantasmas): {df_filas_fantasma.count()}")

# ---------------------------------------------------------------------
# FASE 8: AUDITORÍA DE PATRONES CON EXPRESIONES REGULARES (REGEX)
# ---------------------------------------------------------------------
logger.info("--- 8. AUDITORÍA DE PATRONES (FORMATO SSN) ---")

# Primero, filtramos para ignorar los valores que ya sabemos que son nulos
# isNotNull() asegura que solo evaluemos filas que sí tienen un texto escrito en SSN
df_ssn_existente = df_bronze_analisis.filter(col("SSN").isNotNull())

# Ahora aplicamos la expresión regular para buscar el formato correcto
# col("SSN") es la columna que vamos a evaluar
# r"^(\d{3}-\d{2}-\d{4})$" es el lenguaje de Regex. Significa: 
#   ^  = Inicio del texto
#   \d = Un número del 0 al 9
#   {3}= Exactamente 3 veces
#   -  = Un guion literal
#   $  = Fin del texto
# El número '1' al final significa "extrae el primer grupo que coincida con esta regla"
# Si el SSN es inválido (ej. "ABC-123"), regexp_extract devolverá un string vacío ("")
df_ssn_evaluado = df_ssn_existente.withColumn(
    "ssn_extraido", 
    regexp_extract(col("SSN"), r"^(\d{3}-\d{2}-\d{4})$", 1)
)

# Filtramos aquellos donde el resultado de la extracción fue un texto vacío ("")
# Esto significa que había algo escrito, pero no cumplía el formato numérico con guiones
df_ssn_invalido = df_ssn_evaluado.filter(col("ssn_extraido") == "")

# Contamos cuántos Seguros Sociales son falsos o tienen errores de tipeo
logger.info(f"Seguros Sociales (SSN) con formato corrupto o falso: {df_ssn_invalido.count()}")

logger.info("=== EDA FINALIZADO CORRECTAMENTE ===")