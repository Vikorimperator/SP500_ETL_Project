"""
DAG para el procesamiento ETL (Extract, Transform, Load) de los datos diarios de stocks.

Este DAG está programado para ejecutarse diario a media noche con el objetivo de extraer los datos de stocks,
transformarlos y cargarlos en una base de datos. El flujo de tareas sigue el siguiente orden:
1. Extraer datos
2. Transformar datos
3. Cargar datos

Dependencias: sp500.etl.extract, sp500.etl.transform, sp500.etl.load
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sp500.etl.extract import extract_data
from sp500.etl.transform import transform_data
from sp500.etl.load import load_data

# Argumentos por defecto del DAG, incluye el usuario propietario y la fecha de inicio
default_args = {
    'owner': 'tu_usuario',  # Reemplazar por tu nombre de usuario
    'start_date': datetime(2023, 1, 1),  # Fecha desde la cual se activa el DAG
}

# Definir el contexto del DAG usando un bloque `with` para asegurar una mejor estructura
with DAG('etl_daily_stock_data',
         default_args=default_args,
         schedule_interval='0 0 * * *',  # Ejecutar el DAG a diario a medianoche
         catchup=False  # Evitar que ejecute tareas atrasadas si el DAG está inactivo por un tiempo
         ) as dag:

    # Definir la tarea de extracción de datos mediante PythonOperator
    extract_task = PythonOperator(
        task_id='extract_daily_data',
        python_callable=extract_data  # Llama a la función `extract_data` de la carpeta ETL
    )

    # Definir la tarea de transformación de datos
    transform_task = PythonOperator(
        task_id='transform_daily_data',
        python_callable=transform_data,  # Llama a la función `transform_data` de la carpeta ETL
        provide_context=True  # Proveer el contexto de ejecución (datos entre tareas)
    )

    # Definir la tarea de carga de datos en la base de datos
    load_task = PythonOperator(
        task_id='load_daily_data_to_db',
        python_callable=load_data,  # Llama a la función `load_data` de la carpeta ETL
        provide_context=True  # Proveer el contexto de ejecución (datos entre tareas)
    )

    # Definir el flujo de dependencias entre las tareas
    extract_task >> transform_task >> load_task  # El flujo sigue el orden: extraer -> transformar -> cargar