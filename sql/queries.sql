-- name: MAX_FECHA
SELECT MAX(fecha_origen) FROM raw.frontend_sissa_json;

-- name: TRUNCATE_DESTINO
TRUNCATE TABLE raw.frontend_sissa_json RESTART IDENTITY;

-- name: EXTRACT_BASE
SELECT "Id", "Tipo", "Fecha", "Contenido", "SolicitudId", "ProductoId"
FROM public."Json" 
WHERE "Tipo" = 4004

-- name: INSERT_DESTINO
INSERT INTO raw.frontend_sissa_json (
    id_origen, tipo, fecha_origen, solicitud_id, producto_id, contenido, cuil, documento
) VALUES (
    %(id)s, %(tipo)s, %(fecha)s, %(solicitud_id)s, %(producto_id)s, %(contenido)s, 
    %(cuil)s, %(documento)s
) ON CONFLICT (id_origen) DO NOTHING;