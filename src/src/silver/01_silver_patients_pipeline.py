# ========================================================================
# SCRIPT DE PRODUCCIÓN: CLINICAL DATA LAKEHOUSE - CAPA SILVER
# Objetivo: Limpieza, enmascaramiento de PII y carga incremental (Upsert)
# ========================================================================

import os
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, current_timestamp, when
from delta.tables import DeltaTable 

# ------------------------------------------------------------------------
# 1. CONFIGURACIÓN DE AUDITORÍA (LOGS)
# ------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------
# 2. INICIALIZACIÓN DEL MOTOR
# ------------------------------------------------------------------------
try:
    logger.info("Inicializando Spark con extensiones de Delta Lake...")
    spark = SparkSession.builder \
        .appName("Clinical_Silver_ETL_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
except Exception as e:
    logger.critical(f"Error crítico al iniciar el motor de Spark: {e}")
    sys.exit(1)

# ------------------------------------------------------------------------
# 3. REGLAS DE NEGOCIO CLÍNICAS (LIMPIEZA MODULAR)
# ------------------------------------------------------------------------
def limpiar_pacientes(df):
    # Regla 1: Integridad
    df = df.dropna(subset=["Id"])
    
    # Regla 2: Unicidad
    df = df.dropDuplicates(["Id"])
    
    # Regla 3: Cumplimiento Normativo (PII)
    df = df.drop("SSN", "PASSPORT", "DRIVERS")
    
    # Regla 4: Estandarización de textos y nulos
    # Usamos paréntesis ( ) para un encadenamiento seguro a prueba de errores de indentación
    df = (
        df.withColumn("CITY", trim(upper(col("CITY"))))
          .withColumn("GENDER", trim(upper(col("GENDER"))))
          .withColumn("MARITAL", 
                      when(col("MARITAL").isNull(), "UNKNOWN")
                      .otherwise(trim(upper(col("MARITAL")))))
    )
                       
    # Regla 5: Lógica Temporal Defensiva
    df = df.filter(col("DEATHDATE").isNull() | (col("DEATHDATE") >= col("BIRTHDATE")))
    
    return df

# ------------------------------------------------------------------------
# 4. ENRUTAMIENTO Y LLAVES MAESTRAS
# ------------------------------------------------------------------------
catalogo_bronze = "default.bronze_"
catalogo_silver = "default.silver_"

datasets_clinicos = [
    "patients"
]

llaves_primarias = {
    "patients": ["Id"]
}

# ------------------------------------------------------------------------
# 5. EL MOTOR DE CARGA INCREMENTAL (UPSERT)
# ------------------------------------------------------------------------
def guardar_datos_silver(spark, df, tabla_destino, llaves):
    if spark.catalog.tableExists(tabla_destino):
        logger.info(f"Tabla {tabla_destino} detectada. Ejecutando MERGE incremental...")
        tabla_delta = DeltaTable.forName(spark, tabla_destino)
        
        condicion_merge = " AND ".join([f"target.{pk} = source.{pk}" for pk in llaves])
        
        tabla_delta.alias("target").merge(
            df.alias("source"),
            condicion_merge
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        
    else:
        logger.info(f"Tabla {tabla_destino} no existe. Iniciando Carga Histórica Completa...")
        df.write.format("delta").mode("overwrite").saveAsTable(tabla_destino)

# ------------------------------------------------------------------------
# 6. ORQUESTACIÓN PRINCIPAL DEL PIPELINE
# ------------------------------------------------------------------------
logger.info(f"Iniciando curación de {len(datasets_clinicos)} entidades clínicas...")

for dataset in datasets_clinicos:
    tabla_origen = f"{catalogo_bronze}{dataset}"
    tabla_destino = f"{catalogo_silver}{dataset}"
    
    try:
        logger.info(f"--- Operando entidad: {dataset} ---")
        
        df_crudo = spark.read.table(tabla_origen)
        
        if dataset == "patients":
            df_limpio = limpiar_pacientes(df_crudo)
        else:
            df_limpio = df_crudo
            
        df_final = df_limpio.withColumn("silver_load_date", current_timestamp())
        
        llaves_tabla = llaves_primarias.get(dataset, [])
        
        if not llaves_tabla:
            logger.warning(f"Cuidado: No hay PK para {dataset}. Insertando a ciegas (Append).")
            df_final.write.format("delta").mode("append").saveAsTable(tabla_destino)
        else:
            guardar_datos_silver(spark, df_final, tabla_destino, llaves_tabla)
            
        logger.info(f"✅ Éxito: {dataset} procesado e integrado en {tabla_destino}.")
        
    except Exception as e:
        logger.error(f"❌ Fallo crítico en el pipeline de {dataset}: {e}")
        continue

logger.info("=== PIPELINE SILVER EJECUTADO EN SU TOTALIDAD ===")