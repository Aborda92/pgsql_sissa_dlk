import os
import sys
import json
import logging
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor, execute_values

current_dir = os.path.dirname(os.path.realpath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)
import transformations

# ==========================================
# 1. MÓDULO DE LOGS Y ARGUMENTOS
# ==========================================
def setup_logging(app_path, process_name):
    """Configura la rotación y el formato de los logs."""
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
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
    
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def build_parser():
    """Construye y retorna los argumentos de consola."""
    parser = argparse.ArgumentParser(description="Motor ETL Basado en Metadatos")
    parser.add_argument('--config', type=str, default='conf/config.json', help="Ruta al archivo JSON de configuración")
    parser.add_argument('--mode', type=str, choices=['incr_fecha', 'full'], default='incr_fecha', help="Modo de ejecución")
    return parser.parse_args()

# ==========================================
# 2. MÓDULO DE CARGA DE METADATOS Y QUERIES
# ==========================================
def load_configuration(app_path, config_file):
    """Lee y valida el archivo JSON de configuración."""
    try:
        conf_path = os.path.join(app_path, config_file)
        with open(conf_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Fallo crítico leyendo config.json: {e}")
        sys.exit(1)

def load_queries(app_path, queries_file):
    """Lee el archivo .sql y lo transforma en un diccionario de consultas."""
    queries = {}
    try:
        sql_path = os.path.join(app_path, queries_file)
        with open(sql_path, 'r', encoding='utf-8') as f:
            current_key = None
            for line in f:
                clean_line = line.strip()
                if clean_line.startswith("-- name:"):
                    current_key = clean_line.replace("-- name:", "").strip()
                    queries[current_key] = ""
                elif current_key and clean_line:
                    queries[current_key] += clean_line + " "
        return queries
    except Exception as e:
        logging.error(f"Error crítico cargando archivo SQL: {e}")
        sys.exit(1)

# ==========================================
# 3. MÓDULO DE BASE DE DATOS (Extracción y Carga)
# ==========================================
def build_connection_string(cfg):
    """Construye el string de conexión para PostgreSQL a partir de un diccionario."""
    return f"host={cfg['host']} port={cfg['port']} dbname={cfg['db']} user={cfg['user']} password={cfg['password']}"

def setup_extraction_mode(cur_src, cur_dest, conn_dest, load_mode, queries, config):
    """Prepara el cursor de origen según el modo (Incremental o Full)."""
    if load_mode == "incr_fecha":
        cur_dest.execute(queries["GET_LAST_VALUE_FECHA"])
        result = cur_dest.fetchone()
        last_value = result[0] if result and result[0] is not None else config["incremental"]["default_value"]
        
        logging.info(f"Modo INCR_FECHA: Última fecha procesada = {last_value}")
        cur_src.execute(queries["EXTRACT_INCR_FECHA"], {"last_value": last_value})

    # --- BLOQUE COMENTADO: MODO INCREMENTAL POR ID ---
    # elif load_mode == "incr_id":
    #     cur_dest.execute(queries["GET_LAST_VALUE_ID"])
    #     result = cur_dest.fetchone()
    #     last_value = result[0] if result and result[0] is not None else 0
    #     logging.info(f"Modo INCR_ID: Buscando registros con ID > {last_value}")
    #     cur_src.execute(queries["EXTRACT_INCR_ID"], {"last_value": last_value})
    # -------------------------------------------------

    elif load_mode == "full":
        logging.info("Modo FULL: Ejecutando truncado de tabla destino (PRE_LOAD)...")
        cur_dest.execute(queries["PRE_LOAD"])
        conn_dest.commit()
        logging.info("Extrayendo tabla de origen completa...")
        cur_src.execute(queries["EXTRACT_FULL"])

def process_batches(cur_src, cur_dest, conn_dest, batch_size, transformer_func, query_ev, template_ev):
    """Procesa los registros extraídos en lotes, los transforma y los inserta."""
    total_leidos, total_transformados, total_insertados = 0, 0, 0

    while True:
        registros_raw = cur_src.fetchmany(batch_size)
        if not registros_raw: break
        
        lote_leido = len(registros_raw)
        total_leidos += lote_leido
        
        lote_procesado = []
        for row in registros_raw:
            transformed_row = transformer_func(dict(row))
            if transformed_row:
                lote_procesado.append(transformed_row)
        
        count_transformados = len(lote_procesado)
        total_transformados += count_transformados
        count_ins = 0

        if lote_procesado:
            insertados_lista = execute_values(
                cur_dest, 
                query_ev, 
                lote_procesado, 
                template=template_ev,
                page_size=500,
                fetch=True
            )
            conn_dest.commit()
            
            count_ins = len(insertados_lista) if insertados_lista else 0
            total_insertados += count_ins
            
        logging.info(f"Lote -> Leidos: {lote_leido} | Transf: {count_transformados} | Ins: {count_ins} | Dup: {count_transformados - count_ins}")

    return total_leidos, total_transformados, total_insertados

# ==========================================
# ORQUESTADOR PRINCIPAL
# ==========================================
def main():
    APP_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    
    # 1. Configuración Inicial
    args = build_parser()
    config = load_configuration(APP_PATH, args.config)
    process_name = config.get("process_name", "ETL_GENERICO")
    
    setup_logging(APP_PATH, process_name)
    logging.info("="*60)
    logging.info(f"INICIANDO PROCESO: {process_name} | MODO: {args.mode.upper()}")
    logging.info("="*60)

    # 2. Cargar Dependencias (SQL y Transformador)
    queries = load_queries(APP_PATH, config["queries_file"])
    
    transformer_func = getattr(transformations, config["transformer"], None)
    if not transformer_func:
        logging.error(f"Función transformadora '{config['transformer']}' no encontrada.")
        sys.exit(1)
        
    template_ev = config.get("insert_template")
    if not template_ev:
        logging.error("Falta definir 'insert_template' en el archivo config.json")
        sys.exit(1)

    # 3. Ejecución del Pipeline ETL
    SRC_CONN = build_connection_string(config["source"])
    DEST_CONN = build_connection_string(config["target"])

    try:
        with psycopg2.connect(SRC_CONN) as conn_src, psycopg2.connect(DEST_CONN) as conn_dest:
            with conn_src.cursor(name='cursor_origen', cursor_factory=DictCursor) as cur_src, conn_dest.cursor() as cur_dest:
                
                # Setup y Extracción
                setup_extraction_mode(cur_src, cur_dest, conn_dest, args.mode, queries, config)
                
                # Transformación e Inserción
                leidos, transf, ins = process_batches(
                    cur_src, cur_dest, conn_dest, 
                    config.get("batch_size", 5000), 
                    transformer_func, 
                    queries["INSERT"], 
                    template_ev
                )

        # 4. Reporte Final
        logging.info(
            f"--- FIN DEL PROCESO | "
            f"Leidos={leidos} | "
            f"Transformados={transf} | "
            f"Insertados={ins} | "
            f"Duplicados={transf - ins} ---"
        )

    except Exception as e:
        logging.error(f"Error crítico en el pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()