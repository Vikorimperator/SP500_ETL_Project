"""
DAG para ejecutar el proceso ETL (Extract, Transform, Load) en los datos de stock históricos.

Este DAG realiza las siguientes acciones en secuencia:
1. Extrae los datos de stock
2. Transforma los datos según las reglas definidas
3. Carga los datos transformados en la base de datos

El DAG está configurado para ejecutarse solo una vez.
Dependencias: sp500.etl.extract, sp500.etl.transform, sp500.etl.load
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sp500.etl.extract import extract
from sp500.etl.transform import transform
from sp500.etl.load import load

default_args = {
    'owner': 'tu_usuario',
    'start_date': datetime(2023, 1, 1),
}

with DAG('etl_stock_data',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    # Definir las tareas
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    # Definir el flujo de trabajo
    extract_task >> transform_task >> load_task