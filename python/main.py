# python/main.py
import os
import sys
import json
import logging
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

current_dir = os.path.dirname(os.path.realpath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)
from transformations import parse_sissa_records

def setup_logging(app_path, load_mode):

    log_dir = os.path.join(app_path, 'logs')
    try:
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        archivos_log = [f for f in os.listdir(log_dir) if f.startswith('proceso_sissa_') and f.endswith('.log')]
        archivos_log.sort()
        while len(archivos_log) >= 7:
            os.remove(os.path.join(log_dir, archivos_log.pop(0)))
    except Exception as e:
        print(f"Error gestionando carpeta de logs: {e}")

    log_file = os.path.join(log_dir, f"proceso_sissa_{datetime.today().strftime('%Y%m%d')}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = [] 
    
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)

def parse_arguments(args):
    
    parser = argparse.ArgumentParser(description="Proceso ETL SISSA hacia Datalake PGSQL")
    parser.add_argument('--mode', type=str, required=True, choices=['incremental', 'full'], help="Modo de carga")
    parsed_args = parser.parse_args(args[1:])
    return parsed_args.mode.lower()

def main(args):

    APP_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    LOAD_MODE = parse_arguments(args)

    setup_logging(APP_PATH, LOAD_MODE)
    logging.info("="*60)
    logging.info(f"INICIANDO PROCESO SISSA | MODO: {LOAD_MODE.upper()}")
    logging.info("="*60)


    try:
        CONF_FILE = os.path.join(APP_PATH, 'conf', 'config.json')
        with open(CONF_FILE, 'r') as f:
            config = json.load(f)
            
        SRC_CONN = f"host={config['src_host']} port={config['src_port']} dbname={config['src_db']} user={config['src_user']} password={config['src_pass']}"
        DEST_CONN = f"host={config['dest_host']} port={config['dest_port']} dbname={config['dest_db']} user={config['dest_user']} password={config['dest_pass']}"
        
        SQL_PATH = os.path.join(APP_PATH, 'sql', 'queries.sql')
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
                    
        logging.info("Configuración JSON y SQL cargados correctamente.")
    except Exception as e:
        logging.error(f"Error cargando config/SQL: {e}")
        sys.exit(1)

    query_extraccion = queries["EXTRACT_BASE"]
    try:
        with psycopg2.connect(DEST_CONN) as conn_dest, conn_dest.cursor() as cur_dest:
            if LOAD_MODE == 'incremental':
                cur_dest.execute(queries["MAX_FECHA"])
                max_val = cur_dest.fetchone()[0]
                ultima_fecha = max_val if max_val else '1900-01-01 00:00:00'
                query_extraccion += f" AND \"Fecha\" > '{ultima_fecha}' ORDER BY \"Fecha\" ASC"
                logging.info(f"Objetivo: Extraer registros con fecha > {ultima_fecha}")               
                
            elif LOAD_MODE == 'full':
                logging.info("Truncando tabla destino...")
                cur_dest.execute(queries["TRUNCATE_DESTINO"])
                conn_dest.commit()
                logging.info("Tabla truncada. Se extraerá la tabla origen completa.")
    except Exception as e:
        logging.error(f"Error preparando el modo de carga: {e}")
        sys.exit(1)

    try:
        logging.info("Conectando a bases para iniciar el procesamiento por lotes...")
        with psycopg2.connect(SRC_CONN) as conn_src, psycopg2.connect(DEST_CONN) as conn_dest:
            with conn_src.cursor(name='cursor_extraccion') as cur_src, conn_dest.cursor() as cur_dest:
                cur_src.execute(query_extraccion)
                
                tamaño_lote = 5000
                total_insertados = 0
                
                while True:
                    registros = cur_src.fetchmany(tamaño_lote)
                    if not registros: break
                        
                    lote_procesado = parse_sissa_records(registros)
                    
                    if lote_procesado:
                        execute_batch(cur_dest, queries["INSERT_DESTINO"], lote_procesado, page_size=500)
                        conn_dest.commit()
                        total_insertados += len(lote_procesado)
                        logging.info(f"Procesando... Total acumulado insertado: {total_insertados}")

        if total_insertados == 0:
            logging.info("No hubo registros nuevos para procesar.")
            
        logging.info(f"--- FIN DEL PROCESO SISSA. Total final procesado: {total_insertados} ---")

    except Exception as e:
        logging.error(f"Error crítico en el pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)