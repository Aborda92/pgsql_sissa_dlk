# python/main.py
import os
import logging
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

# 1. Definir rutas y configuración de directorios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = os.path.join(BASE_DIR, '..', 'conf', 'config.json')
SQL_PATH = os.path.join(BASE_DIR, '..', 'sql', 'queries.sql')
LOG_DIR = os.path.join(BASE_DIR, '..', 'log')

# 2. Configurar el Logger ANTES de cualquier otra operación
if not os.path.exists(LOG_DIR): 
    os.makedirs(LOG_DIR)

logger = logging.getLogger("ETL_SISSA")
logger.setLevel(logging.INFO)
log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

if not logger.handlers:
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())

# 3. Borrar logs antiguos (más de 7 días)
try:
    fecha_limite = datetime.now() - timedelta(days=7)
    for archivo in os.listdir(LOG_DIR):
        ruta_archivo = os.path.join(LOG_DIR, archivo)
        if os.path.isfile(ruta_archivo) and archivo.endswith(".log"):
            fecha_archivo = datetime.strptime(archivo.replace(".log", ""), "%Y-%m-%d")
            if fecha_archivo < fecha_limite:
                os.remove(ruta_archivo)
except Exception as e:
    logger.warning(f"No se pudo limpiar logs antiguos: {e}")

# 4. Cargar configuración
with open(CONF_PATH) as f:
    config = json.load(f)

SRC_CONN = f"host={config['SRC']['HOST']} port={config['SRC']['PORT']} dbname={config['SRC']['DB']} user={config['SRC']['USER']} password={config['SRC']['PASS']}"
DEST_CONN = f"host={config['DEST']['HOST']} port={config['DEST']['PORT']} dbname={config['DEST']['DB']} user={config['DEST']['USER']} password={config['DEST']['PASS']}"

# 5. Cargar Queries desde archivo .sql
queries = {}
try:
    with open(SQL_PATH, 'r', encoding='utf-8') as f:
        current_key = None
        for line in f:
            # Quitamos espacios laterales y saltos de línea (\r y \n)
            clean_line = line.strip()
            
            # Buscamos la etiqueta ignorando si hay espacios antes o después
            if clean_line.startswith("-- name:"):
                # Extraemos el nombre y lo limpiamos de cualquier residuo
                current_key = clean_line.replace("-- name:", "").strip()
                queries[current_key] = ""
            elif current_key and clean_line:
                # Si la línea no es una etiqueta, es parte del query
                queries[current_key] += clean_line + " "
    
    # Debug: Imprimir lo que realmente cargó
    logger.info(f"Queries cargadas: {list(queries.keys())}")
    
    if "QUERY_ULTIMA_FECHA" not in queries:
        raise ValueError("No se encontró la etiqueta QUERY_ULTIMA_FECHA")

except Exception as e:
    logger.error(f"Error crítico cargando el archivo SQL: {e}")
    raise

def procesar_ingesta():
    try:
        logger.info("--- INICIANDO NUEVA EJECUCIÓN BATCH ---")
        
        with psycopg2.connect(DEST_CONN) as conn_dest, conn_dest.cursor() as cur_dest:
            cur_dest.execute(queries["QUERY_ULTIMA_FECHA"])
            res = cur_dest.fetchone()
            ultima_fecha = res[0] if res and res[0] else '1900-01-01'
            
        logger.info(f"Buscando registros en Origen posteriores a: {ultima_fecha}")

        with psycopg2.connect(SRC_CONN) as conn_src, conn_src.cursor() as cur_src:
            cur_src.execute(queries["QUERY_EXTRACCION"], {"ultima_fecha": ultima_fecha})
            registros = cur_src.fetchall()
            
        if not registros:
            logger.info("No hay registros nuevos para procesar.")
            logger.info("--- FIN DE LA EJECUCIÓN ---")
            return

        logger.info(f"Se extrajeron {len(registros)} registros nuevos. Preparando lote...")

        lote = []
        for row in registros:
            id_o, tipo, fecha, cont, sol_id, prod_id = row
            c_json = json.loads(cont) if isinstance(cont, str) else cont
            
            # Rescate multinivel
            base = c_json if "DatosSalida" in c_json else c_json.get("FullResponse", {})
            datos_salida = base.get("DatosSalida", {})
            datos_entrada = base.get("DatosEntrada", {})
            
            personas = datos_salida.get("Personas", [])
            duplicados = datos_salida.get("Duplicados", [])
            fuente = personas[0] if personas else (duplicados[0] if duplicados else None)
            
            if fuente:
                cuil = str(fuente.get("Cuil")) if fuente.get("Cuil") else None
                documento = str(fuente.get("NroDoc")) if fuente.get("NroDoc") else None
            else:
                cuil, documento = None, str(datos_entrada.get("Documento"))

            lote.append({"id": id_o, "tipo": tipo, "fecha": fecha, "solicitud_id": sol_id, 
                         "producto_id": prod_id, "contenido": json.dumps(c_json), "cuil": cuil, "documento": documento})

        with psycopg2.connect(DEST_CONN) as conn_dest, conn_dest.cursor() as cur_dest:
            execute_batch(cur_dest, queries["QUERY_INSERCION"], lote, page_size=500)
            conn_dest.commit()
            
        logger.info(f"Ingesta finalizada: {len(lote)} registros procesados.")
        logger.info("--- FIN DE LA EJECUCIÓN ---")

    except Exception as e:
        logger.error(f"Error crítico en la ingesta: {str(e)}")

if __name__ == "__main__":
    procesar_ingesta()