# Proyecto ETL - Carga de Datos Históricos y Actualización Diaria

Este proyecto tiene como objetivo desarrollar un pipeline de ETL (Extract, Transform, Load) que permite la carga de datos históricos de acciones bursátiles y realiza actualizaciones diarias de los datos. El ETL está diseñado para integrarse con una base de datos PostgreSQL y es ejecutado automáticamente utilizando Airflow.

## Contenidos

- [Descripción del Proyecto](#descripción-del-proyecto)
- [Instalación](#instalación)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Licencia](#licencia)

## Descripción del Proyecto

El pipeline ETL se divide en dos fases principales:

1. **Carga de datos históricos**: Extrae, transforma y carga un conjunto de datos históricos en una base de datos PostgreSQL.
2. **Actualización diaria**: Cada día, el pipeline extrae nuevos datos y los integra en la base de datos, manteniéndola actualizada.

El proyecto se apoya en Airflow para la automatización del proceso y SQLAlchemy para las interacciones con la base de datos.

## Instalación

Sigue los siguientes pasos para instalar y ejecutar el proyecto:

1. Clona el repositorio:
```bash
git clone https://github.com/tuusuario/proyecto-etl.git
cd proyecto-etl
```

2. Crea un entorno virtual y actívalo:
```bash
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate  # Windows
```

3. Instala las dependencias:
```bash
pip install -r requirements.txt
```

4. Configura las variables de entorno. Crea un archivo .env en la raíz del proyecto y define las siguientes variables:
```python
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_de_tu_base_de_datos
STOCK_DATA_CSV_PATH=ruta_a_tus_datos_historicos.csv
STOCK_EXTRAC_DATA_TEMP=/tmp/stock_data_extracted.csv
STOCK_TRANSFORM_DATA_TEMP=/tmp/stock_data_transformed.csv
DAILY_STOCK_DATA=ruta_a_los_datos_diarios
DAILY_STOCK_EXTRACT_DATA_TEMP=/tmp/stock_daily_extracted.csv
```
## Uso

1. Ejecutar el pipeline ETL para cargar datos historicos:

```bash
airflow dags trigger etl_stock_data.py
```

2. Actualizar los datos diariamente:

El pipeline de Airflow ejecutará automáticamente la actualización diaria. Puedes ejecutar manualmente con:

```bash
airflow dags trigger etl_daily_stock_data
```

El DAG de Airflow se encarga de extraer nuevos datos diarios, transformarlos y cargarlos en la base de datos.

## Estructura del Proyecto

```bash
kiosko_challange/
│
├── airflow/
│   └── dags/
│       ├── etl_stock_data.py  # DAG la carga de datos historicos
│       └── etl_daily_stock_data.py  # DAG la carga de datos diariamente
│
├── data/
│   ├── raw/  # Datos crudos
│   ├── processed/  # Datos transformados
│
├── sp500/
│   ├── db/
│   │   └── db_config.py  # Configuración de la base de datos
│   └── etl/
│       ├── extract.py  # Extracción de datos
│       ├── transform.py  # Transformación de datos
│       └── load.py  # Carga de datos en la base de datos
│
├── .env  # Variables de entorno
├── requirements.txt  # Dependencias del proyecto
└──  README.md  # Descripción del proyecto
```

## Licencia

Este proyecto está bajo la licencia MIT.

### Explicación del `README.md`:
1. **Descripción del Proyecto**: Detalla brevemente qué hace el proyecto y cómo está estructurado el pipeline ETL.
2. **Instalación**: Proporciona instrucciones claras sobre cómo clonar el proyecto, configurar el entorno, e instalar las dependencias.
3. **Uso**: Explica cómo ejecutar tanto la carga de datos históricos como la actualización diaria.
4. **Estructura del Proyecto**: Da una vista general de cómo están organizados los archivos dentro del proyecto.
5. **Licencia**: Detalles sobre la licencia del proyecto.
