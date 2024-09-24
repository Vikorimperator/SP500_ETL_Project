from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import text
from sp500.db.db_config import get_session, close_session
from sp500.etl.extract import extract_data

# Cargar las variables de entorno desde el archivo .env
load_dotenv()
TEMP_EXTRACT_DATA = os.getenv('STOCK_EXTRAC_DATA_TEMP')  # Ruta temporal de los datos extraídos
TEMP_TRANSFORM_DATA = os.getenv('STOCK_TRANSFORM_DATA_TEMP')  # Ruta temporal para los datos transformados
TRANSFORMED_FILE_PATH = os.getenv('DAILY_STOCK_TRANSFORM_DATA_TEMP')  # Ruta temporal para el archivo transformado

def transform():
    """
    Realiza la transformación de los datos extraídos, incluyendo limpieza de datos numéricos y fechas.

    Lee los datos desde un archivo CSV, convierte las columnas numéricas y de fechas al formato correcto,
    rellena valores faltantes y elimina filas con valores no numéricos. Luego guarda los datos transformados 
    en un archivo CSV temporal.
    """
    df = pd.read_csv(TEMP_EXTRACT_DATA)

    # Transformación de las fechas y conversión de columnas numéricas
    df['date'] = pd.to_datetime(df['date'])
    numeric_columns = ['open', 'high', 'low', 'close']
    
    # Convertir las columnas numéricas a valores válidos y rellenar valores faltantes
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df[numeric_columns].ffill(inplace=True)
    
    # Asegurar que la columna 'volume' sea de tipo int64
    df['volume'] = df['volume'].astype('int64')

    # Eliminar filas con valores NaN en las columnas numéricas
    df.dropna(subset=numeric_columns, inplace=True)

    # Guardar el DataFrame transformado en un archivo CSV temporal
    df.to_csv(TEMP_TRANSFORM_DATA, index=False)
    print(f'Datos transformados y guardados en {TEMP_TRANSFORM_DATA}.')
    
def get_latest_dates():
    """
    Obtiene las fechas más recientes registradas en la base de datos para cada compañía.

    Retorna:
        dict: Un diccionario con el nombre de la compañía como clave y la última fecha como valor.
    """
    session = get_session()
    
    # Consulta SQL para obtener la fecha más reciente por compañía
    query = """
    SELECT c.name, MAX(sp.date) as latest_date
    FROM stock_prices sp
    JOIN companies c ON sp.company_id = c.id
    GROUP BY c.name;
    """
    
    try:
        # Ejecutar la consulta y construir el diccionario con los resultados
        result = session.execute(text(query))
        latest_dates_dict = {row['name']: row['latest_date'] for row in result}
        return latest_dates_dict
    finally:
        # Cerrar la sesión de la base de datos
        close_session(session)
        
def transform_data(**context):
    """
    Toma los datos extraídos y los transforma, filtrando por fechas.

    Obtiene los datos transformados del archivo generado por `extract_data`, filtra los registros
    cuya fecha es posterior a la última registrada en la base de datos, y guarda los datos filtrados 
    en un archivo temporal.

    Args:
        context (dict): Diccionario de contexto proporcionado por Airflow para intercambiar datos entre tareas.

    Retorna:
        str: La ruta del archivo CSV con los datos transformados o `None` si no hay datos para transformar.
    """
    # Obtener la ruta del archivo generado por extract_data a través de XCom
    extracted_file = context['task_instance'].xcom_pull(task_ids='extract_daily_data')
    
    if extracted_file:
         # Leer los datos extraídos desde el archivo CSV
        latest_stock_df = pd.read_csv(extracted_file)
        
        # Obtener las fechas más recientes de la base de datos para cada compañía
        latest_dates_dict = get_latest_dates()
        
        # Filtrar los datos para incluir solo los registros con fechas más recientes que las registradas
        filtered_df = latest_stock_df[latest_stock_df.apply(
            lambda row: pd.Timestamp(row['date']) > pd.Timestamp(latest_dates_dict.get(row['name'], '1900-01-01')), axis=1)]
        
        # Guardar el DataFrame filtrado en un archivo CSV temporal
        filtered_df.to_csv(TRANSFORMED_FILE_PATH, index=False)
        
        # Retornar la ruta del archivo con los datos transformados
        return TRANSFORMED_FILE_PATH
    else:
        # Si no se encontraron datos extraídos, retornar None
        return None