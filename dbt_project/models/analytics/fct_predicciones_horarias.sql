{{ config(materialized='table', schema='analytics') }}

SELECT
    municipio,
    fecha_hora::date as fecha,                 
    EXTRACT(HOUR FROM fecha_hora) as hora,     
    temperatura,
    sens_termica,
    humedad,     
    precipitacion,
    prob_precipitacion,
    estado_cielo_desc as estado_cielo,
    fecha_hora
FROM {{ ref('stg_predicciones') }}
ORDER BY municipio, fecha_hora ASC