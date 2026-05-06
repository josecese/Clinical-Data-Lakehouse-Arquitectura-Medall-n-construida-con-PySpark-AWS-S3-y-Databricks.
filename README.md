🏥 Clinical Data Lakehouse Project
¡Bienvenido al repositorio del proyecto Clinical Data Lakehouse! 🚀

Este proyecto demuestra una solución integral de ingeniería de datos para el sector salud, utilizando una arquitectura moderna de Lakehouse para procesar datos clínicos complejos, garantizando la calidad del dato y el cumplimiento de normativas de privacidad.

🏗️ Arquitectura de Datos
El proyecto sigue la Medallion Architecture para garantizar un flujo de datos organizado y auditable:

Capa Bronze: Almacena los datos crudos (Raw) ingeridos desde archivos Parquet/CSV. Es nuestra "fuente de verdad" inmutable.

Capa Silver: Capa de curación donde aplicamos limpieza, estandarización de tipos, manejo de nulos y enmascaramiento de datos sensibles (PII). Aquí es donde el EDA (Exploratory Data Analysis) se transforma en reglas de negocio.

Capa Oro: Datos listos para el negocio, modelados en un esquema de estrella (Star Schema) para alimentar dashboards de Power BI o modelos de Machine Learning.

📖 Resumen Técnico
Este proyecto destaca habilidades avanzadas en:

Ingeniería de Datos: Pipelines escalables con PySpark y Databricks.

Almacenamiento Moderno: Implementación de Delta Lake para garantizar transacciones ACID y cargas incrementales (Upserts/Merge).

Calidad del Dato: Perfilado profundo con Regex y validaciones lógicas de negocio.

Infraestructura como Código: Configuración de entorno con Terraform y orquestación con Airflow.

🚀 Requisitos y Alcance
Ingeniería de Datos
Fuentes de Datos: Integración de sistemas clínicos (Pacientes, Encuentros, Medicaciones) en formato Parquet.

Calidad: Limpieza rigurosa de duplicados y resolución de inconsistencias temporales (ej. fechas de defunción vs nacimiento).

Seguridad: Eliminación estricta de SSN, Pasaportes y Licencias en cumplimiento con estándares de privacidad.

Integración: Consolidación de fuentes en un modelo de datos relacional optimizado para analítica.

Análisis y BI
Comportamiento del Paciente: Análisis de recurrencia en visitas médicas.

Métricas Clínicas: Tendencias de prescripción médica y demografía poblacional.

📂 Estructura del Repositorio
Siguiendo las mejores prácticas de la industria:

Plaintext
clinical-data-lakehouse/
│
├── Airflow/                # Orquestación de DAGs para los procesos ETL
├── terraform/              # Infraestructura como Código (AWS/Azure)
├── notebooks/              # Versiones experimentales de análisis en Databricks
│
├── src/                    # Lógica principal del Pipeline (Producción)
│   ├── bronze/             # Ingesta y Script Maestro de Exploración (EDA)
│   ├── silver/             # Limpieza, Upserts con Delta Lake y Calidad
│   └── gold/               # Modelado de tablas de Hechos y Dimensiones
│
├── docs/                   # Documentación de Arquitectura y Diccionario de Datos
├── tests/                  # Pruebas unitarias para validar las transformaciones
└── README.md               # Resumen del proyecto