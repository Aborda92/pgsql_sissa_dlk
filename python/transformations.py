# python/transformations.py
import json
import logging

def parse_sissa_records(registros):
    """
    Recibe los registros crudos de BD, parsea el JSON y devuelve una lista de diccionarios
    lista para ser insertada por execute_batch.
    """
    lote = []
    for row in registros:
        try:
            id_o, tipo, fecha, cont, sol_id, prod_id = row
            
            if not cont:
                logging.warning(f"Fila ID {id_o} ignorada: El campo 'Contenido' está vacío.")
                continue

            c_json = json.loads(cont) if isinstance(cont, str) else cont
            
            base = c_json if "DatosSalida" in c_json else c_json.get("FullResponse", {})
            datos_salida = base.get("DatosSalida", {})
            datos_entrada = base.get("DatosEntrada", {})
            
            personas = datos_salida.get("Personas", [])
            duplicados = datos_salida.get("Duplicados", [])
            fuente = personas[0] if personas else (duplicados[0] if duplicados else None)
            
            cuil_raw = None
            doc_raw = None
            
            if fuente:
                cuil_raw = fuente.get("Cuil")
                doc_raw = fuente.get("NroDoc")
                a
            if not cuil_raw and datos_entrada:
                cuil_raw = datos_entrada.get("Cuil")
                
            if not doc_raw and datos_entrada:
                doc_raw = datos_entrada.get("Documento")

            cuil_final = str(cuil_raw) if cuil_raw else None
            doc_final = str(doc_raw) if doc_raw else None

            lote.append({
                "id": id_o, 
                "tipo": tipo, 
                "fecha": fecha, 
                "solicitud_id": sol_id, 
                "producto_id": prod_id, 
                "contenido": json.dumps(c_json), 
                "cuil": cuil_final, 
                "documento": doc_final
            })
            
        except Exception as e:

            logging.warning(f"Error transformando fila ID {row[0] if len(row) > 0 else 'Desconocido'}: {e}")
            continue
        
    return lote