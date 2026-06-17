# python/main.py
import os
import logging
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = os.path.join(BASE_DIR, '..', 'conf', 'config.json')
SQL_PATH = os.path.join(BASE_DIR, '..', 'sql', 'queries.sql')
LOG_DIR = os.path.join(BASE_DIR, '..', 'log')

if not os.path.exists(LOG_DIR): 
    os.makedirs(LOG_DIR)

with open(CONF_PATH, 'r') as f:
    config = json.load(f)

NOMBRE_PROCESO = config.get("PROCESS_NAME", "ETL_SISSA")

logger = logging.getLogger(NOMBRE_PROCESO)
logger.setLevel(logging.INFO)
log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

if not logger.handlers:
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())

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

# Conexiones
SRC_CONN = f"host={config['SRC']['HOST']} port={config['SRC']['PORT']} dbname={config['SRC']['DB']} user={config['SRC']['USER']} password={config['SRC']['PASS']}"
DEST_CONN = f"host={config['DEST']['HOST']} port={config['DEST']['PORT']} dbname={config['DEST']['DB']} user={config['DEST']['USER']} password={config['DEST']['PASS']}"

queries = {}
try:
    with open(SQL_PATH, 'r', encoding='utf-8') as f:
        current_key = None
        for line in f:
            clean_line = line.strip()
            if clean_line.startswith("-- name:"):
                current_key = clean_line.replace("-- name:", "").strip()
                queries[current_key] = ""
            elif current_key and clean_line:
                queries[current_key] += clean_line + " "
    
    if "QUERY_ULTIMA_FECHA" not in queries:
        raise ValueError("No se encontró la etiqueta QUERY_ULTIMA_FECHA")
except Exception as e:
    logger.error(f"Error crítico cargando el archivo SQL: {e}")
    raise

def procesar_ingesta():

    logger.info("="*60)
    logger.info(f"ARRANCANDO PROCESO: {NOMBRE_PROCESO}")
    logger.info(f"Fecha de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    try:
        with psycopg2.connect(DEST_CONN) as conn_dest, conn_dest.cursor() as cur_dest:
            cur_dest.execute(queries["QUERY_ULTIMA_FECHA"])
            res = cur_dest.fetchone()
            ultima_fecha = res[0] if res and res[0] else '1900-01-01'
            
        logger.info(f"Buscando registros posteriores a: {ultima_fecha}")

        with psycopg2.connect(SRC_CONN) as conn_src, conn_src.cursor() as cur_src:
            cur_src.execute(queries["QUERY_EXTRACCION"], {"ultima_fecha": ultima_fecha})
            registros = cur_src.fetchall()
            
        if not registros:
            logger.info("No hay registros nuevos. Terminando.")
            logger.info("--- FIN DE LA EJECUCIÓN ---")
            return

        logger.info(f"Se extrajeron {len(registros)} registros nuevos. Preparando lote...")

        lote = []
        for row in registros:
            id_o, tipo, fecha, cont, sol_id, prod_id = row
            c_json = json.loads(cont) if isinstance(cont, str) else cont
            
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