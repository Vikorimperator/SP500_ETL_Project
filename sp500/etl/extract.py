from dotenv import load_dotenv
import pandas as pd
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()
INPUT_DATA = os.getenv('STOCK_DATA_CSV_PATH') # Ruta del archivo CSV principal de datos de stocks
TEMP_EXTRACT_DATA = os.getenv('STOCK_EXTRAC_DATA_TEMP') # Ruta temporal para guardar los datos extraídos
DAILY_DATA= os.getenv('DAILY_STOCK_DATA') # Ruta de la carpeta que contiene los datos diarios
EXTRACTED_FILE_PATH = os.getenv('DAILY_STOCK_EXTRACT_DATA_TEMP')  # Ruta temporal para los datos extraídos diariamente

def extract():
    """
    Extrae los datos de un archivo CSV, los guarda en una ruta temporal y valida si el archivo existe.
    
    Excepciones:
        FileNotFoundError: Si el archivo CSV especificado no existe.
    """
    csv_path = os.path.join(INPUT_DATA)
    
    # Verificar si el archivo existe, si no, lanzar una excepción
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'El archivo {csv_path} no existe.')
    
    # Leer el archivo CSV y guardarlo en la ruta temporal
    df = pd.read_csv(csv_path)
    df.to_csv(TEMP_EXTRACT_DATA, index=False) # Guardar los datos extraídos en un archivo temporal
    print(f"Datos extraídos y guardados en {TEMP_EXTRACT_DATA}") # Confirmación de la extracción
    
def extract_data():
    """
    Carga y concatena datos desde múltiples archivos CSV diarios en un DataFrame.

    Esta función lee archivos CSV de una carpeta diaria, los concatena en un solo DataFrame, 
    convierte las columnas a minúsculas, convierte la columna de fechas a tipo datetime 
    y guarda el resultado en un archivo CSV temporal.

    Retorna:
        str: La ruta del archivo CSV combinado, o None si no se encontraron archivos CSV.
    """
    dataframes = []
    
    # Recorrer la carpeta de datos diarios para cargar archivos CSV
    for filename in os.listdir(DAILY_DATA):
        if filename.endswith('.csv'): # Filtrar solo archivos con extensión .csv
            file_path = os.path.join(DAILY_DATA, filename)
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.lower()  # Convertir los nombres de las columnas a minúsculas
            
            # Verificar si existe la columna 'date' y convertirla a tipo datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convertir la columna 'date'
                
            dataframes.append(df)  # Añadir el DataFrame a la lista
    
    if dataframes:
        # Concatenar todos los DataFrames cargados en uno solo
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Guardar el DataFrame combinado en un archivo CSV temporal
        combined_df.to_csv(EXTRACTED_FILE_PATH, index=False)
        
        # Devolver la ruta del archivo CSV donde se guardaron los datos
        return EXTRACTED_FILE_PATH
    else:
        # Si no se encontraron archivos, retornar None
        return None