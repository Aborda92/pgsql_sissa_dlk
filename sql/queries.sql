-- name: GET_LAST_VALUE_FECHA
SELECT COALESCE(MAX(fecha_origen), '1900-01-01 00:00:00') FROM raw.frontend_sissa_json;

-- name: PRE_LOAD
TRUNCATE TABLE raw.frontend_sissa_json RESTART IDENTITY;

-- name: EXTRACT_FULL
SELECT "Id" AS id, "Tipo" AS tipo, "Fecha" AS fecha, "Contenido" AS contenido, "SolicitudId" AS solicitud_id, "ProductoId" AS producto_id
FROM public."Json" 
WHERE "Tipo" = 4004;

-- name: EXTRACT_INCR_FECHA
SELECT "Id" AS id, "Tipo" AS tipo, "Fecha" AS fecha, "Contenido" AS contenido, "SolicitudId" AS solicitud_id, "ProductoId" AS producto_id 
FROM public."Json" 
WHERE "Tipo" = 4004 AND "Fecha" >= %(last_value)s - interval '5 minutes'
ORDER BY "Fecha" ASC;

-- name: INSERT
INSERT INTO raw.frontend_sissa_json (
    id_origen, tipo, fecha_origen, solicitud_id, producto_id, contenido, cuil, documento
) VALUES %s 
ON CONFLICT (id_origen) DO NOTHING 
RETURNING id_origen;