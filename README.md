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

2. Ejecutar el contenedor (requiere el parámetro `--mode`, si no se especifica, utiliza incr_fecha por defecto): 
   
   **Para carga diaria o diferencial (Solo trae registros nuevos por fecha):**
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py --mode incr_fecha`
   
   **Para carga diaria o diferencial (Solo trae registros nuevos por id, en este proceso no aplica):**
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py --mode incr_id`   
   
   **Para recarga completa (Trunca la tabla destino y carga todo desde cero):**
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py --mode full`

## Configuración
Crea el archivo `conf/config.json` en la raíz del proyecto con la siguiente estructura plana:

```json
{
    "process_name": "PGSQL_SISSA_DLK",
    "source": {
        "host": "10.19.110.80",
        "port": 5432,
        "db": "postgres",
        "user": "etluser",
        "password": "tu_password"
    },
    "target": {
        "host": "10.19.115.31",
        "port": 5432,
        "db": "postgres",
        "user": "etluser",
        "password": "tu_password"
    },
    "incremental": {
        "field": "fecha_origen",
        "default_value": "1900-01-01 00:00:00"
    },
    "queries_file": "sql/queries.sql",
    "transformer": "sissa_json",
    "batch_size": 5000
    "insert_template": "(%(id)s, %(tipo)s, %(fecha)s, %(solicitud_id)s, %(producto_id)s, %(contenido)s, %(cuil)s, %(documento)s)"	
}

## Nota sobre el desarrollo
Este proyecto utiliza un patrón de diseño Config-Driven. Para agregar un proceso nuevo:

Crea un nuevo archivo .json en conf/.

Define las queries necesarias en sql/.

Si la lógica de transformación cambia, añade la función correspondiente en python/transformations.py.