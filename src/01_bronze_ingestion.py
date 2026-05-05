import logging # Importamos la librería nativa de Python para generar un registro de eventos (bitácora)
import sys # Importamos sys para poder detener el programa abruptamente si ocurre un error fatal
from pyspark.sql import SparkSession # Importamos el constructor de sesiones de Spark
from pyspark.sql.functions import current_timestamp, lit, to_date # Importamos las herramientas de transformación de columnas
import boto3 # Importamos el SDK de Amazon para conectarnos a S3 saltando las restricciones
import pandas as pd # Importamos Pandas (corregido de 'ps' a 'pd') para leer los CSV en memoria
from io import BytesIO # Importamos BytesIO para tratar los datos de la memoria como archivos físicos

# ========================================================================
# PASO 1: CONFIGURACIÓN DE LOGS (BITÁCORA)
# ========================================================================

# Configuramos el formato estándar de la empresa para los mensajes del sistema
logging.basicConfig(
    level=logging.INFO, # Establecemos que solo queremos ver mensajes informativos, advertencias y errores (nada de relleno)
    format='%(asctime)s - %(levelname)s - %(message)s' # Definimos el esqueleto: Fecha - Nivel (INFO/ERROR) - Mensaje
)

# Creamos nuestro objeto 'logger' basándonos en el nombre del archivo actual (__name__ en minúsculas)
logger = logging.getLogger(__name__)

# ========================================================================
# PASO 2: INICIO DE SESIÓN Y CONFIGURACIÓN
# ========================================================================

try: # Iniciamos una zona de control de errores crítica
    logger.info("Encendiendo el motor principal de Spark...") # Dejamos un registro de que el proceso arrancó
    spark = SparkSession.builder.getOrCreate() # Busca una sesión de Spark activa en Databricks, si no hay, la crea
except Exception as e: # Si Spark no enciende (ej. clúster apagado)
    logger.critical(f"Fallo crítico al iniciar Spark: {e}") # Usamos f-string (faltaba la 'f') para registrar el error
    sys.exit(1) # Forzamos el apagado del script para no gastar recursos a lo tonto

# SIMULACIÓN DE SEGURIDAD EMPRESARIAL: 
# En producción usaríamos: ACCESS_KEY = dbutils.secrets.get(scope="aws", key="access_key")
# Por ahora, ponemos las variables (¡PON TUS LLAVES NUEVAS AQUÍ, PERO NO LAS COMPARTAS!)
ACCESS_KEY = "REEMPLAZAR_POR_LLAVE_SOLO_EN_DATABRICKS"
SECRET_KEY = "REEMPLAZAR_POR_CLAVE_SOLO_EN_DATABRICKS"
BUCKET_NAME = "proyecto-synthea-bronze-josecese" # Tu caja fuerte en S3
ruta_destino_base = "dbfs:/FileStore/capa_bronze/" # Ruta interna de Databricks donde guardaremos el Parquet

# Lista de las tablas exactas que vamos a procesar
datasets = ['patients.csv', 'encounters.csv', 'conditions.csv', 'medications.csv']

# Creamos el puente de conexión directa con AWS S3 (Corregido: faltaba una coma)
s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# ========================================================================
# PASO 3: INGESTA, TRANSFORMACIÓN Y CARGA (BUCLE)
# ========================================================================

# Iniciamos el bucle: repetiremos todo este bloque por cada archivo en la lista 'datasets'
for archivo in datasets: 
    try: # Abrimos una zona segura por cada archivo. Si uno falla, los demás siguen.
        logger.info(f"--- Iniciando extracción de {archivo} desde S3 ---") # Logeamos qué archivo empezamos
        
        # Le pedimos a AWS que nos entregue el paquete de datos específico
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=archivo)
        
        # Leemos el paquete en RAM y lo convertimos en un DataFrame de Pandas
        # Corregido: Es obj['Body'] con corchetes, no paréntesis
        df_pandas = pd.read_csv(BytesIO(obj['Body'].read())) 
        
        # ¡LA MAGIA! Entregamos la tabla de Pandas al motor de Big Data (PySpark)
        df_spark = spark.createDataFrame(df_pandas)
        
        # TRANSFORMACIÓN (Metadatos de Auditoría)
        logger.info(f"Aplicando metadatos de auditoría a {archivo}...") # Log de seguimiento
        
        # Agregamos 3 columnas nuevas al DataFrame de Spark (Corregido: df_spark en lugar de df_pandas)
        df_bronze = df_spark \
            .withColumn("fecha_ingesta_bronze", current_timestamp()) \
            .withColumn("archivo_origen", lit(archivo)) \
            .withColumn("particion_fecha", to_date(current_timestamp()))
            # Nota de Linaje: Usamos 'lit(archivo)' porque 'input_file_name()' falla en este bypass de Python
        
        # Limpiamos el nombre para la carpeta (ej. "patients.csv" -> "patients")
        nombre_tabla = archivo.replace('.csv', '')
        
        # Creamos la ruta final exacta donde se guardará este archivo en formato Parquet
        ruta_parquet = f"{ruta_destino_base}{nombre_tabla}"
        
        # CARGA (Escritura en el Catálogo SQL de Databricks)
        nombre_tabla_sql = f"default.bronze_{nombre_tabla}"
        logger.info(f"Guardando {nombre_tabla} como tabla administrada: {nombre_tabla_sql}...")
        
        # Tomamos el DataFrame finalizado y le damos la orden de escritura
        df_bronze.write \
            .mode("append") \
            .partitionBy("particion_fecha") \
            .saveAsTable(nombre_tabla_sql)
            # Guardamos agregando datos nuevos (append), dividiendo en carpetas por fecha (partitionBy), en formato Parquet.
            
        logger.info(f"✅ ¡ÉXITO! {archivo} procesado y guardado en: {nombre_tabla_sql}\n") # Log de victoria
        
    except Exception as e: # Si este CSV en particular falla (ej. AWS no responde o el archivo no existe)
        logger.error(f"❌ Fallo al procesar {archivo}. Motivo: {e}\n") # Registramos el error en la bitácora
        continue # Ignoramos el fallo de este archivo y obligamos al bucle a intentar con el siguiente de la lista

# Mensaje final que indica que el bucle terminó de revisar todos los elementos
logger.info("=== INGESTA MASIVA A CAPA BRONZE FINALIZADA ===")