import unittest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sp500.db.db_config import get_session, engine

class TestDatabaseConnection(unittest.TestCase):
    
    def test_conexion_exitosa(self):
        """
        Prueba que la conexión a la base de datos se pueda establecer exitosamente.

        Ejecuta una consulta simple para verificar si la base de datos responde correctamente. Si no se puede
        establecer la conexión, el test fallará.
        """
        try:
            # Intentar conectar a la base de datos y ejecutar una consulta simple
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1")).scalar()
                # Verificar que el resultado de la consulta sea 1
                self.assertEqual(result, 1, "La conexión no fue exitosa.")
        except OperationalError as e:
            # En caso de error, el test fallará con un mensaje detallado
            self.fail(f"Error al conectar a la base de datos: {e}")
    
    def test_session_creation(self):
        """
        Prueba la creación y cierre de una sesión.

        Verifica que se puede crear una sesión de SQLAlchemy correctamente y que se puede cerrar después de su uso.
        """
        session = get_session()
        try:
            # Verificar que la sesión fue creada exitosamente
            self.assertIsNotNone(session, "La sesión no fue creada correctamente.")
        finally:
            # Asegurarse de que la sesión siempre se cierra después de la prueba
            session.close()

if __name__ == '__main__':
    unittest.main()