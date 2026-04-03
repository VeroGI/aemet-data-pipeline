{{ config(materialized='view', schema='staging') }}

WITH raw_data AS (
    
    SELECT * FROM {{ source('aemet', 'prediccion_horaria') }}
)

SELECT
    municipio,
    codigo_ine,
    -- Lógica de timestamp  
    (split_part(fecha, 'T', 1) || ' ' || hora || ':00:00')::timestamp as fecha_hora,
    -- Limpieza de nulos y conversión de tipos
    NULLIF(temperatura, '')::float as temperatura,
    NULLIF(humedad, '')::float as humedad,
    NULLIF(precipitacion, '')::float as precipitacion,
    NULLIF(sens_termica, '')::float as sens_termica,
    estado_cielo_desc,
    NULLIF(prob_precipitacion, '')::float as prob_precipitacion,
    -- Columna de auditoría 
    current_timestamp as cargado_en_staging_at
FROM raw_data