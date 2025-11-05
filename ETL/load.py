import pandas as pd
import os
# La importación relativa funciona porque este módulo se ejecuta
# como parte del paquete 'ETL' (importado por el orquestador)
from .extract import TARGET_DW_DIR

def load_to_csv(df_to_save, filename):
    """
    Persiste un DataFrame a un archivo .csv dentro del directorio
    del Data Warehouse (DW).
    
    Asegura la compatibilidad con herramientas BI (ej: Power BI)
    forzando el separador decimal a PUNTO (.).
    
    Args:
        df_to_save (pd.DataFrame): DataFrame a guardar.
        filename (str): Nombre del archivo de destino (ej: 'dim_customer.csv').
    """
    try:
        # Se garantiza la existencia del directorio de destino (DW)
        os.makedirs(TARGET_DW_DIR, exist_ok=True)
        
        file_path_full = os.path.join(TARGET_DW_DIR, filename)
        
        # Guardado en CSV
        df_to_save.to_csv(
            file_path_full, 
            index=False, # No guardar el índice de pandas
            decimal='.', # FORZAR el uso del punto como separador decimal (CRUCIAL para BI)
            encoding='utf-8' # Estándar de codificación
        )
        
        print(f"   -> Persistencia exitosa en: {file_path_full}")
        
    except Exception as e:
        print(f"Error al guardar el archivo {filename} en DW: {e}")

if __name__ == '__main__':
    # Script de verificación de carga
    print("Iniciando verificación de la función de persistencia...")
    # Se utiliza una variable de ruta corregida para este módulo (si se ejecuta solo)
    # NOTA: Para este ejemplo se usaría TARGET_DW_DIR importado
    
    # Crear un DataFrame de prueba con decimales
    test_df = pd.DataFrame({'value_usd': [1234.56, 789.0], 'label': ['Test1', 'Test2']})
    load_to_csv(test_df, 'verification_table.csv')
    print("Verificación de persistencia finalizada.")
    print(f"Revisa 'verification_table.csv' en /dw para confirmar el formato.")