# PGSQL SISSA DLK
Motor de ingesta de datos para el sistema SISSA.

## Cómo ejecutar localmente
1. Construir la imagen (ejecutar en la raíz del proyecto): 
   `docker build -t pgsql_sissa_dlk .`
2. Ejecutar: 
   `docker run --rm -v "${PWD}:/app" pgsql_sissa_dlk python /app/python/main.py`

## Configuración
Crea el archivo `conf/config.json` con la siguiente estructura:
```json
{
  "PROCESS_NAME": "PGSQL_SISSA_DLK",
  "SRC": {
    "HOST": "...", 
    "PORT": 5432, 
    "DB": "...", 
    "USER": "...", 
    "PASS": "..."
  },
  "DEST": {
    "HOST": "...", 
    "PORT": 5432, 
    "DB": "...", 
    "USER": "...", 
    "PASS": "..."
  }
}