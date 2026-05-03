# ==============================================================================
# 1. BLOQUE DE CONFIGURACIÓN PRINCIPAL DE TERRAFORM
# ==============================================================================
# Este bloque le dice a Terraform qué "plugins" o proveedores necesita descargar
# para poder comunicarse con la nube.
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws" # Descarga el proveedor oficial de AWS creado por HashiCorp
      version = "~> 5.0"        # Usa la versión 5.x (la más reciente y segura), evitando cambios bruscos
    }
  }
}

# ==============================================================================
# 2. CONFIGURACIÓN DEL PROVEEDOR (AWS)
# ==============================================================================
# Aquí configuramos dónde queremos que AWS construya nuestra infraestructura.
provider "aws" {
  region = "us-east-1" # Norte de Virginia. Es la región estándar de AWS, suele tener todas las herramientas disponibles y es económica.
}

# ==============================================================================
# RECURSOS DE ARQUITECTURA MEDALLION (S3 BUCKETS)
# ==============================================================================

# ------------------------------------------------------------------------------
# CAPA BRONZE: El Aterrizaje de Datos Crudos
# ------------------------------------------------------------------------------

# Crea el "balde" (bucket) físico en S3
resource "aws_s3_bucket" "bronze_lake" {
  bucket        = "proyecto-synthea-bronze-josecese" # El nombre debe ser único en todo el mundo.
  force_destroy = true # IMPORTANTE: Permite borrar el bucket desde la terminal aunque tenga archivos adentro. (En una empresa real se pone en 'false' para evitar desastres).
  
  # Las etiquetas ayudan a las empresas a saber de quién es este recurso y cuánto cuesta
  tags = {
    Ambiente = "Desarrollo"
    Capa     = "Bronze"
    Proyecto = "Synthea Lakehouse"
  }
}

# REGLA DE SEGURIDAD 1: Encriptación en reposo
# Obliga a AWS a encriptar cualquier archivo que caiga aquí usando el algoritmo estándar AES-256.
resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_crypto" {
  bucket = aws_s3_bucket.bronze_lake.id # Se vincula al ID del bucket que creamos arriba
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# REGLA DE SEGURIDAD 2: Bloqueo de Acceso Público (BPA)
# Cierra absolutamente todas las puertas de internet. Nadie puede ver estos archivos médicos por URL, solo a través de credenciales estrictas (como las de Databricks).
resource "aws_s3_bucket_public_access_block" "bronze_seguridad" {
  bucket                  = aws_s3_bucket.bronze_lake.id
  block_public_acls       = true # Bloquea Listas de Control de Acceso públicas
  block_public_policy     = true # Bloquea políticas de bucket públicas
  ignore_public_acls      = true # Ignora cualquier intento de hacer un archivo público individualmente
  restrict_public_buckets = true # Restringe el bucket completo a solo usuarios autenticados
}


# ------------------------------------------------------------------------------
# CAPA SILVER: Datos Limpios y Filtrados (Formato Delta)
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "silver_lake" {
  bucket        = "proyecto-synthea-silver-josecese"
  force_destroy = true
  
  tags = {
    Ambiente = "Desarrollo"
    Capa     = "Silver"
    Proyecto = "Synthea Lakehouse"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_crypto" {
  bucket = aws_s3_bucket.silver_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver_seguridad" {
  bucket                  = aws_s3_bucket.silver_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# ------------------------------------------------------------------------------
# CAPA GOLD: Modelado Analítico y Métricas de Negocio
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "gold_lake" {
  bucket        = "proyecto-synthea-gold-josecese"
  force_destroy = true
  
  tags = {
    Ambiente = "Desarrollo"
    Capa     = "Gold"
    Proyecto = "Synthea Lakehouse"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_crypto" {
  bucket = aws_s3_bucket.gold_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold_seguridad" {
  bucket                  = aws_s3_bucket.gold_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}