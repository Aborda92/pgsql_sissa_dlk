# PGSQL SISSA DLK
Motor de ingesta de datos para el sistema SISSA. Extrae registros desde PostgreSQL, transforma los datos JSON y los ingesta en un Datalake PostgreSQL utilizando procesamiento por lotes.

## Estructura del Proyecto
* `conf/`: Contiene el archivo de configuración con las credenciales (`config.json`).
* `sql/`: Centraliza todas las consultas SQL (`queries.sql`) separadas por etiquetas.
* `python/`: Contiene el orquestador principal (`main.py`) y la lógica de parseo JSON (`transformations.py`).
* `logs/`: Directorio autogenerado donde se guardan los historiales de ejecución.

## Cómo ejecutar localmente (docker)
1. Construir la imagen (ejecutar en la raíz del proyecto): 
   `docker build -t pgsql_sissa_dlk .`

2. Ejecutar el contenedor (requiere el parámetro `--mode`): 
   
   **Para carga diaria o diferencial (Solo trae registros nuevos por fecha):**
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py --mode incremental`
   
   **Para recarga completa (Trunca la tabla destino y carga todo desde cero):**
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py --mode full`

## Configuración
Crea el archivo `conf/config.json` en la raíz del proyecto con la siguiente estructura plana:

```json
{
    "app_name": "PGSQL_SISSA_DLK",
    "src_host": "10.0.0.1",
    "src_port": "5432",
    "src_db": "base_origen",
    "src_user": "usuario_origen",
    "src_pass": "password_origen",
    "dest_host": "10.0.0.2",
    "dest_port": "5432",
    "dest_db": "base_destino",
    "dest_user": "usuario_destino",
    "dest_pass": "password_destino"
}