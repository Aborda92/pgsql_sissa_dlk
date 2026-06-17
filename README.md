# PSGR SISSA ETL
Sistema de ingesta de datos para el sistema SISSA.

## Cómo ejecutar localmente
1. Construir la imagen: `docker build -t psgr-sissa-test .`
2. Ejecutar: `docker run --rm -v "${PWD}:/app" psgr-sissa-test python /app/python/main.py`

## Configuración
Requiere un archivo `conf/config.json` con la estructura:
```json
{
  "SRC": {"HOST": "...", "PORT": 5432, "DB": "...", "USER": "...", "PASS": "..."},
  "DEST": {"HOST": "...", "PORT": 5432, "DB": "...", "USER": "...", "PASS": "..."}
}
