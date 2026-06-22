# PGSQL SISSA DLK
Motor ETL universal basado en metadatos para la ingesta de datos entre bases de datos PostgreSQL. Diseñado para alta disponibilidad, procesamiento por lotes y escalabilidad mediante configuración externa.

## Estructura del Proyecto
* `conf/`: Contiene los archivos de configuración JSON (ej. `config.json`).
* `sql/`: Centraliza todas las consultas SQL (`queries.sql`) parametrizadas.
* `python/`: Contiene el orquestador principal (`main.py`) y el módulo de transformaciones dinámicas (`transformations.py`).
* `logs/`: Directorio autogenerado para el monitoreo de ejecuciones.

## Cómo ejecutar localmente (docker)
1. **Construir la imagen:**
   ```bash
   docker build -t pgsql_sissa_dlk .
   
   
2. Ejecutar el contenedor:
El motor acepta el parámetro --mode para definir la estrategia de carga. Si no se especifica, utiliza incr_fecha por defecto.

Carga incremental por fecha (Recomendada para procesos diarios):

docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk --mode incr_fecha
Carga incremental por ID:

docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk --mode incr_id
Recarga completa (Trunca destino y carga todo):

docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk --mode full

## Configuración
Crea el archivo conf/config.json en la raíz del proyecto con la estructura requerida por el motor:

JSON
{
    "process_name": "PGSQL_SISSA_DLK",
    "source": {
        "host": "10.19.110.80",
        "port": 5432,
        "db": "UNZ.Ob_backend.D",
        "user": "dev_user",
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
}

## Nota sobre el desarrollo
Este proyecto utiliza un patrón de diseño Config-Driven. Para agregar un proceso nuevo:

Crea un nuevo archivo .json en conf/.

Define las queries necesarias en sql/.

Si la lógica de transformación cambia, añade la función correspondiente en python/transformations.py.