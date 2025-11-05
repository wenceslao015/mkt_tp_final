import pandas as pd
import os

# Definición de rutas absolutas basadas en la ubicación del script
# Esto establece las ubicaciones de los datos crudos y del Data Warehouse (DW)
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DATA_DIR = os.path.join(BASE_PATH, 'raw')
TARGET_DW_DIR = os.path.join(BASE_PATH, 'dw')

# Lista de fuentes (archivos .csv) a procesar
CSV_SOURCES = [
    'address',
    'channel',
    'customer',
    'nps_response',
    'payment',
    'product',
    'product_category',
    'province',
    'sales_order',
    'sales_order_item',
    'shipment',
    'store',
    'web_session'
]

def extract_all_data(source_dir=SOURCE_DATA_DIR):
    """
    Obtiene y carga todas las fuentes de datos (archivos .csv) desde un 
    directorio específico hacia un diccionario de DataFrames de pandas.
    
    Args:
        source_dir (str): Directorio donde se encuentran los archivos fuente.
        
    Returns:
        dict: Diccionario {nombre_tabla: DataFrame} o None si ocurre un error.
    """
    data_container = {}
    print(f"Localizando y cargando datos desde el path: {source_dir}")
    
    try:
        for table_name in CSV_SOURCES:
            ruta_archivo = os.path.join(source_dir, f"{table_name}.csv")
            # Se utiliza la función de lectura de CSV de pandas
            data_container[table_name] = pd.read_csv(ruta_archivo)
            print(f"  -> Fuente '{table_name}' integrada correctamente.")
            
        print("Etapa de recolección de datos finalizada.\n")
        return data_container
    
    except FileNotFoundError as e:
        print(f"Error de acceso: No se localizó el archivo o directorio. {e}")
        return None
    except Exception as e:
        print(f"Error inesperado durante la recolección: {e}")
        return None

if __name__ == '__main__':
    # Script de verificación para asegurar que el módulo funciona independientemente.
    raw_data = extract_all_data()
    if raw_data:
        print("\nVerificación de Extracción exitosa.")
        print(f"Fuentes procesadas: {list(raw_data.keys())}")
        print(f"\nEsquema de las primeras 5 filas de 'customer':")
        print(raw_data['customer'].head())