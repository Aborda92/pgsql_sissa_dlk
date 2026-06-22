import os
import sys
import json
import logging
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch, DictCursor

current_dir = os.path.dirname(os.path.realpath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)
import transformations

def setup_logging(app_path, process_name):
    log_dir = os.path.join(app_path, 'logs')
    try:
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        archivos_log = [f for f in os.listdir(log_dir) if f.startswith(f'{process_name}_') and f.endswith('.log')]
        archivos_log.sort()
        while len(archivos_log) >= 7:
            os.remove(os.path.join(log_dir, archivos_log.pop(0)))
    except Exception as e:
        print(f"Error gestionando carpeta de logs: {e}")

    log_file = os.path.join(log_dir, f"{process_name}_{datetime.today().strftime('%Y%m%d')}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = [] 
    
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)

def main():
    APP_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    
    parser = argparse.ArgumentParser(description="Motor ETL Basado en Metadatos")
    parser.add_argument('--config', type=str, default='conf/config.json', help="Ruta al archivo JSON de configuración")
    parser.add_argument('--mode', type=str, choices=['incr_fecha', 'incr_id', 'full'], default='incr_fecha', help="Modo de ejecución")
    args = parser.parse_args()

    # 1. Leer Configuración
    try:
        CONF_FILE = os.path.join(APP_PATH, args.config)
        with open(CONF_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        process_name = config.get("process_name", "ETL_GENERICO")
        load_mode = args.mode
        batch_size = config.get("batch_size", 5000)
    except Exception as e:
        print(f"Fallo crítico leyendo config: {e}")
        sys.exit(1)

    setup_logging(APP_PATH, process_name)
    logging.info("="*60)
    logging.info(f"INICIANDO PROCESO: {process_name} | MODO: {load_mode.upper()}")
    logging.info("="*60)

    # 2. Cargar Consultas SQL
    try:
        SQL_PATH = os.path.join(APP_PATH, config["queries_file"])
        queries = {}
        with open(SQL_PATH, 'r', encoding='utf-8') as f:
            current_key = None
            for line in f:
                clean_line = line.strip()
                if clean_line.startswith("-- name:"):
                    current_key = clean_line.replace("-- name:", "").strip()
                    queries[current_key] = ""
                elif current_key and clean_line:
                    queries[current_key] += clean_line + " "
    except Exception as e:
        logging.error(f"Error cargando archivo SQL: {e}")
        sys.exit(1)

    # 3. Mapear función transformadora dinámicamente
    transformer_func = getattr(transformations, config["transformer"], None)
    if not transformer_func:
        logging.error(f"No se encontró la función transformadora '{config['transformer']}'.")
        sys.exit(1)

    # 4. Strings de conexión
    SRC_CONN = f"host={config['source']['host']} port={config['source']['port']} dbname={config['source']['db']} user={config['source']['user']} password={config['source']['password']}"
    DEST_CONN = f"host={config['target']['host']} port={config['target']['port']} dbname={config['target']['db']} user={config['target']['user']} password={config['target']['password']}"

    # 5. Ejecutar Motor ETL
    try:
        with psycopg2.connect(SRC_CONN) as conn_src, psycopg2.connect(DEST_CONN) as conn_dest:
            
            with conn_src.cursor(name='cursor_origen', cursor_factory=DictCursor) as cur_src, conn_dest.cursor() as cur_dest:
                

                if load_mode == "incr_fecha":
                    cur_dest.execute(queries["GET_LAST_VALUE_FECHA"])
                    result = cur_dest.fetchone()
                    last_value = result[0] if result and result[0] is not None else config["incremental"]["default_value"]
                    logging.info(f"Buscando registros con Fecha > {last_value}")
                    cur_src.execute(queries["EXTRACT_INCR_FECHA"], {"last_value": last_value})

                elif load_mode == "incr_id":
                    cur_dest.execute(queries["GET_LAST_VALUE_ID"])
                    result = cur_dest.fetchone()

                    last_value = result[0] if result and result[0] is not None else 0
                    logging.info(f"Buscando registros con ID > {last_value}")
                    cur_src.execute(queries["EXTRACT_INCR_ID"], {"last_value": last_value})

                elif load_mode == "full":
                    logging.info("Ejecutando truncado de tabla destino (PRE_LOAD)...")
                    cur_dest.execute(queries["PRE_LOAD"])
                    conn_dest.commit()
                    logging.info("Extrayendo tabla de origen completa...")
                    cur_src.execute(queries["EXTRACT_FULL"])


                total_insertados = 0
                while True:
                    registros_raw = cur_src.fetchmany(batch_size)
                    if not registros_raw: break
                    
                    lote_procesado = []
                    for row in registros_raw:
                        row_dict = dict(row)
                        transformed_row = transformer_func(row_dict)
                        if transformed_row:
                            lote_procesado.append(transformed_row)
                    
                    if lote_procesado:
                        execute_batch(cur_dest, queries["INSERT"], lote_procesado, page_size=500)
                        conn_dest.commit()
                        total_insertados += len(lote_procesado)
                        logging.info(f"Procesando... Total acumulado: {total_insertados}")

        if total_insertados == 0:
            logging.info("No hubo registros nuevos para procesar.")
            
        logging.info(f"--- FIN DEL PROCESO. Total final: {total_insertados} ---")

    except Exception as e:
        logging.error(f"Error crítico en el pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()