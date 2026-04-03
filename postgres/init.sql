-- Crear los esquemas para cada capa del Data Warehouse
CREATE SCHEMA IF NOT EXISTS raw;        -- Bronze: Datos brutos de la API
CREATE SCHEMA IF NOT EXISTS staging;    -- Silver: Vistas limpias de dbt
CREATE SCHEMA IF NOT EXISTS analytics;  -- Gold: Tablas finales para dashboards

-- Crear la tabla Raw (necesaria para que el TRUNCATE de Python no falle)
CREATE TABLE IF NOT EXISTS raw.prediccion_horaria (
    municipio TEXT,
    codigo_ine TEXT,
    fecha TEXT,
    hora TEXT,
    temperatura TEXT,
    humedad TEXT,
    precipitacion TEXT,
    sens_termica TEXT,
    estado_cielo_desc TEXT,
    prob_precipitacion TEXT,
    insertado_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Permisos para el usuario de Airflow
ALTER SCHEMA raw OWNER TO airflow;
ALTER SCHEMA staging OWNER TO airflow;
ALTER SCHEMA analytics OWNER TO airflow;
ALTER TABLE raw.prediccion_horaria OWNER TO airflow;