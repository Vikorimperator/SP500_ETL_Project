import logging
import os

# Crear una carpeta para guardar los logs si no existe
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # Crear el directorio 'logs' si no está presente

def setup_logging(log_file='etl_process.log'):
    """
    Configura el sistema de logging para el proceso ETL.

    Este método crea un archivo de log en el directorio especificado y define
    el formato del mensaje y el nivel de los logs. Los mensajes de log se guardan
    con formato de fecha y hora.

    Args:
        log_file (str): Nombre del archivo donde se guardarán los logs (opcional, por defecto 'etl_process.log').
    """
    # Definir la ruta completa del archivo de log
    log_path = os.path.join(log_dir, log_file)

    # Configurar las opciones de logging
    logging.basicConfig(
        filename=log_path,  # Ruta del archivo donde se guardarán los logs
        level=logging.INFO,  # Nivel de logging: INFO (puede ser modificado a DEBUG, ERROR, etc.)
        format='%(asctime)s - %(levelname)s - %(message)s',  # Formato del mensaje de log con nivel y timestamp
        datefmt='%Y-%m-%d %H:%M:%S'  # Formato de la fecha y hora en los logs
    )