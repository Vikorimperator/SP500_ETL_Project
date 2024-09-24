from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import text
from sp500.db.db_config import get_session, close_session

# Cargar las variables de entorno desde el archivo .env
load_dotenv()
TEMP_TRANSFORM_DATA = os.getenv('STOCK_TRANSFORM_DATA_TEMP') # Ruta temporal de los datos transformados

def load():
    """
    Carga los datos transformados desde un archivo CSV en la base de datos.

    Lee los datos de `TEMP_TRANSFORM_DATA`, inserta los datos de compañías en la tabla `companies` y
    los precios de las acciones en la tabla `stock_prices`. Si ocurre un error de integridad o SQL, se 
    realiza un rollback y se maneja la excepción adecuadamente.
    """
    session = get_session()

    try:
        # Leer el archivo CSV con los datos transformados
        df = pd.read_csv(TEMP_TRANSFORM_DATA)
        session.begin() # Iniciar una nueva transacción

        # Iterar sobre las filas del DataFrame
        for index, row in df.iterrows():
            # Verificar si la compañía ya existe en la base de datos
            company_result = session.execute(
                "SELECT id FROM companies WHERE name = :name", {'name': row['Name']}
            ).fetchone()

            # Si la compañía no existe, insertarla y obtener su `company_id`
            if company_result is None:
                session.execute(
                    "INSERT INTO companies (name) VALUES (:name)", {'name': row['Name']}
                )
                company_id = session.execute(
                    "SELECT id FROM companies WHERE name = :name", {'name': row['Name']}
                ).fetchone()[0]
            else:
                company_id = company_result[0]

            # Insertar los datos de stock en la tabla `stock_prices`
            session.execute(
                """
                INSERT INTO stock_prices (company_id, date, open, high, low, close, volume)
                VALUES (:company_id, :date, :open, :high, :low, :close, :volume)
                """,
                {
                    'company_id': company_id,
                    'date': row['date'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }
            )

        session.commit() # Confirmar la transacción
        print("Datos cargados exitosamente en la base de datos.")

    except IntegrityError:
        # Manejar errores de integridad, como duplicados
        session.rollback()
        print("Error de integridad durante la transacción, se ha hecho rollback.")

    except SQLAlchemyError as e:
        # Manejar cualquier otro error de SQLAlchemy
        session.rollback()
        print(f"Ocurrió un error durante la carga de datos: {e}")

    finally:
        # Cerrar la sesión de la base de datos
        close_session(session)
        
def load_data(**context):
    """
    Carga los datos de stock transformados y los inserta en la base de datos.

    Este método obtiene la ruta del archivo transformado desde XCom (compartido por otras tareas de Airflow),
    y luego inserta los datos en las tablas `companies` y `stock_prices`.

    Args:
        context (dict): Diccionario de contexto proporcionado por Airflow para obtener información
        entre tareas del DAG.
    """
    # Obtener la ruta del archivo generado por la tarea de transformación
    transformed_file = context['task_instance'].xcom_pull(task_ids='transform_daily_data')
    
    if transformed_file:
        # Leer los datos transformados desde el archivo CSV
        new_stock_data = pd.read_csv(transformed_file)

        if not new_stock_data.empty:
            session = get_session()

            try:
                # Insertar las compañías en la base de datos
                for company in new_stock_data['name'].unique():
                    query = """
                    INSERT INTO companies (name)
                    VALUES (:company)
                    ON CONFLICT (name) DO NOTHING
                    """
                    session.execute(text(query), {"company": company})
                
                # Insertar los precios de acciones en la tabla `stock_prices`
                for _, row in new_stock_data.iterrows():
                    # Obtener el `company_id` de la tabla `companies`
                    query = """
                    SELECT id FROM companies WHERE name = :company_name
                    """
                    company_id = session.execute(text(query), {"company_name": row['name']}).scalar()

                    # Insertar los datos de acciones en `stock_prices`
                    query = """
                    INSERT INTO stock_prices (company_id, date, open, high, low, close, volume)
                    VALUES (:company_id, :date, :open, :high, :low, :close, :volume)
                    """
                    session.execute(text(query), {
                        "company_id": company_id,
                        "date": row['date'],
                        "open": row['open'],
                        "high": row['high'],
                        "low": row['low'],
                        "close": row['close'],
                        "volume": row['volume']
                    })
                
                # Confirmar los cambios en la base de datos
                session.commit()
            except Exception as e:
                # En caso de error, realizar un rollback de la transacción
                session.rollback()
                raise e
            finally:
                # Cerrar la sesión
                close_session(session)
        else:
            print("No hay datos nuevos para insertar.")
    else:
        print("No se encontró el archivo transformado.")