from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Configuración
MUNICIPIOS = {
    "43096": "La Morera de Montsant",
    "43168": "Vilanova de Prades",
    "43015": "Arbolí",
    "43049": "Siurana (Cornudella)",
    "43075": "Margalef",
}
NUMERIC_COLS = ["temperatura", "humedad", "precipitacion", "sens_termica", "prob_precipitacion"]

# 2. Funciones de Apoyo 
def _get_connection_string(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    host = "aemet-postgres-1" if conn.host in ["localhost", "127.0.0.1"] else conn.host
    return f"postgresql+psycopg2://{conn.login}:{conn.password}@{host}:{conn.port}/{conn.schema}"

def _init_row(municipio: str, cod: str, fecha: str, hora: int) -> dict:
    return {
        "municipio": municipio, "codigo_ine": cod, "fecha": fecha, "hora": f"{hora:02d}",
        "temperatura": None, "humedad": None, "precipitacion": None,
        "sens_termica": None, "estado_cielo_desc": "", "prob_precipitacion": None,
    }

def _fill_hourly(horas: dict, items: list, col: str, is_desc: bool = False):
    for item in items or []:
        h = item.get("periodo")
        if h in horas:
            if is_desc:
                horas[h][col] = item.get("descripcion", "")
            else:
                val = item.get("value")
                horas[h][col] = float(val) if val not in (None, "") else None

def _fill_prob_precip(horas: dict, items: list):
    for item in items or []:
        periodo = item.get("periodo", "")
        if len(periodo) == 4:
            inicio, fin = periodo[:2], periodo[2:]
            val = item.get("value")
            for h in horas:
                if inicio <= h < fin:
                    horas[h]["prob_precipitacion"] = float(val) if val else None

def _fetch_municipio(cod: str, nombre: str, api_key: str) -> list[dict]:
    url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{cod}"
    res = requests.get(url, params={"api_key": api_key}, timeout=30)
    res.raise_for_status()
    data = res.json()
    if data.get("estado") != 200:
        raise ValueError(f"API error {data.get('estado')} para {nombre}")
    
    forecast_res = requests.get(data["datos"], timeout=30)
    forecast = forecast_res.json()
    if not forecast: return []
    
    rows = []
    for dia in forecast[0].get("prediccion", {}).get("dia", []):
        fecha = dia.get("fecha", "").split("T")[0]
        # Creamos el diccionario de horas 00-23
        horas_dict = {f"{h:02d}": _init_row(nombre, cod, fecha, h) for h in range(24)}
        _fill_hourly(horas_dict, dia.get("temperatura", []), "temperatura")
        _fill_hourly(horas_dict, dia.get("humedadRelativa", []), "humedad")
        _fill_hourly(horas_dict, dia.get("precipitacion", []), "precipitacion")
        _fill_hourly(horas_dict, dia.get("sensTermica", []), "sens_termica")
        _fill_hourly(horas_dict, dia.get("estadoCielo", []), "estado_cielo_desc", is_desc=True)
        _fill_prob_precip(horas_dict, dia.get("probPrecipitacion", []))
        rows.extend(horas_dict.values())
    return rows

# 3. Función Principal de la Tarea
def extract_and_load(**context):
    api_key = BaseHook.get_connection("aemet_api").password
    engine = create_engine(_get_connection_string("weather_postgres"))
    
    # Preparamos el esquema y vaciamos la tabla sin borrarla  
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        # TRUNCATE limpia los datos. Usamos CASCADE por si hay vistas de dbt bloqueando.
        conn.execute(text("TRUNCATE TABLE raw.prediccion_horaria RESTART IDENTITY CASCADE;"))

    all_rows = []
    for cod, nombre in MUNICIPIOS.items():
        try:
            rows = _fetch_municipio(cod, nombre, api_key)
            all_rows.extend(rows)
            print(f"✓ {nombre}: {len(rows)} filas extraídas")
        except Exception as e:
            print(f"✗ {nombre}: Error -> {e}")

    if not all_rows:
        raise ValueError("No se pudieron extraer datos de ningún municipio.")

    df = pd.DataFrame(all_rows)
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Usamos if_exists="append" porque ya hicimos el TRUNCATE arriba. 
   
    df.to_sql(
        "prediccion_horaria", 
        engine, 
        schema="raw", 
        if_exists="append", 
        index=False, 
        method="multi"
    )
    print(f"Éxito: {len(df)} filas insertadas en raw.prediccion_horaria")
    return len(df)

# 4. El DAG
with DAG(
    dag_id="aemet_medallion_v2_final",
    default_args={
        "owner": "data-team", 
        "retries": 1, 
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aemet', 'medallion'],
) as dag:

    extract = PythonOperator(
        task_id="extract_to_raw", 
        python_callable=extract_and_load
    )
    
    transform = BashOperator(
        task_id="transformar_con_dbt",
        bash_command='''
            cd /opt/airflow/dbt/dbt_project && dbt run --profiles-dir .
        ''',
    )

    extract >> transform