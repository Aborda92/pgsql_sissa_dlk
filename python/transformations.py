import json
import logging

def sissa_json(row):

    try:
        contenido = row.get("contenido")
        
        if not contenido:
            logging.warning(f"Fila ID {row.get('id')} ignorada: El campo 'contenido' está vacío.")
            return None

        c_json = json.loads(contenido) if isinstance(contenido, str) else contenido
        
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
            
        if not cuil_raw and datos_entrada:
            cuil_raw = datos_entrada.get("Cuil")
            
        if not doc_raw and datos_entrada:
            doc_raw = datos_entrada.get("Documento")

        row["cuil"] = str(cuil_raw) if cuil_raw else None
        row["documento"] = str(doc_raw) if doc_raw else None
        row["contenido"] = json.dumps(c_json)
        
        return row
        
    except Exception as e:
        logging.warning(f"Error transformando fila ID {row.get('id', 'Desconocido')}: {e}")
        return None