import time
from ETL.extract import extract_all_data
from ETL.transform import transform_all_data
from ETL.load import load_to_csv

def main():
    """Ejecuta el proceso ETL completo para el proyecto EcoBottle."""
    print("🚀 Iniciando proceso ETL - Proyecto EcoBottle\n")

    start_time = time.time()
    success = True

    try:
        # Fase 1: Extracción
        print("[1/3] Extrayendo datos desde /RAW...")
        raw_data = extract_all_data()
        if not raw_data:
            print("❌ No se pudieron obtener los datos. Proceso detenido.")
            success = False
        else:
            print("✅ Extracción finalizada.\n")

        # Fase 2: Transformación
        if success:
            print("[2/3] Transformando datos al modelo de almacén...")
            dw_tables = transform_all_data(raw_data)
            if not dw_tables:
                print("❌ Error durante la transformación. Proceso detenido.")
                success = False
            else:
                print("✅ Transformación completada.\n")

        # Fase 3: Carga
        if success:
            print("[3/3] Cargando datos procesados en /DW...")
            num_tables = 0

            for name, df in dw_tables.items():
                load_to_csv(df, f"{name}.csv")
                num_tables += 1
                print(f"   - Archivo generado: {name}.csv")

            print("\n✅ Carga finalizada con éxito.")
            print(f"📂 Se generaron {num_tables} tablas en la carpeta /DW.\n")

        # Tiempo total
        total_time = time.time() - start_time
        print(f"⏱️  Tiempo total de ejecución: {total_time:.2f} segundos")
        print("🎉 Proceso ETL completado correctamente")

    except Exception as e:
        print("\n⚠️  Error inesperado durante la ejecución del ETL")
        print(f"Detalle técnico: {e}")

if __name__ == "__main__":
    main()
