import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las credenciales de la base de datos desde las variables de entorno
DB_USER = os.getenv("DB_USER")  # Usuario de la base de datos
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Contraseña de la base de datos
DB_HOST = os.getenv("DB_HOST")  # Dirección del host de la base de datos
DB_PORT = os.getenv("DB_PORT")  # Puerto de conexión a la base de datos
DB_NAME = os.getenv("DB_NAME")  # Nombre de la base de datos

# Crear la cadena de conexión a la base de datos PostgreSQL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear el engine de SQLAlchemy, que se encarga de gestionar la conexión con la base de datos
engine = create_engine(DATABASE_URL, future=True)

# Crear una fábrica de sesiones usando sessionmaker, la cual se usará para manejar las transacciones con la base de datos
Session = sessionmaker(bind=engine)

def get_session():
    """
    Crea una nueva sesión de base de datos.

    Retorna:
        session (Session): Objeto de sesión de SQLAlchemy que permite interactuar con la base de datos.
    """
    return Session()

def close_session(session):
    """
    Cierra la sesión proporcionada.

    Args:
        session (Session): Objeto de sesión de SQLAlchemy que debe cerrarse.
    """
    session.close()

