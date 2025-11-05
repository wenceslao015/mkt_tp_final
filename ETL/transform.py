# Módulo: etl.transform
# Descripción: Colección de funciones para la fase de Transformación (T) 
# en el proceso ETL. Su objetivo es convertir los datos crudos (tablas relacionales)
# en un modelo dimensional (Dimensiones y Tablas de Hechos) listo para el Data Warehouse.

import pandas as pd
import numpy as np # Importamos numpy por si se usa en alguna operación interna, aunque pandas ya lo incluye.

# --- Funciones Auxiliares ---

def _get_date_id(date_series, dim_calendar):
    """
    Función auxiliar para buscar la Surrogate Key (SK) de la dimensión de calendario
    basado en una serie de fechas (timestamps).
    
    Aplica un LEFT JOIN entre la serie de fechas y dim_calendar para obtener el 'id' (SK).
    
    Devuelve NaN si la fecha no se encuentra en el calendario.
    
    Args:
        date_series (pd.Series): La serie de timestamps o fechas a mapear.
        dim_calendar (pd.DataFrame): La dimensión de calendario completa (ya creada).
        
    Returns:
        pd.Series: Una serie con las SK 'id' de dim_calendar, lista para ser una FK en la tabla de hechos.
    """
    # Normaliza la fecha de entrada (elimina la hora para el join con dim_calendar)
    dates = pd.to_datetime(date_series, errors='coerce').dt.normalize()
    dates_df = pd.DataFrame({'date_lookup': dates})
    
    # --- MERGE (LEFT JOIN) ---
    # Busca el 'id' (SK) en dim_calendar usando la fecha normalizada como clave.
    merged = pd.merge(
        dates_df,
        dim_calendar[['date', 'id']],
        left_on='date_lookup',
        right_on='date',
        how='left'
    )
    return merged['id']

def _get_time(date_series):
    """
    Función auxiliar para extraer el componente de tiempo (HH:MM:SS) de una serie de fechas/timestamps.
    
    Rellena los valores Nulos (NaN) con '00:00:00'.
    
    Args:
        date_series (pd.Series): La serie de timestamps.
        
    Returns:
        pd.Series: Una serie de strings con el formato de hora 'HH:MM:SS'.
    """
    times = pd.to_datetime(date_series, errors='coerce')
    return times.dt.strftime('%H:%M:%S').fillna('00:00:00')


# --- Funciones de Creación de Dimensiones ---

def create_dim_calendar(data):
    """
    Crea la Dimensión de Calendario (dim_calendar) DINÁMICAMENTE.
    
    El rango de fechas se determina a partir de la unión de todas las fechas
    existentes en las tablas de datos crudos proporcionadas.
    
    Args:
        data (dict): Diccionario con los DataFrames de datos crudos.
        
    Returns:
        pd.DataFrame: La dimensión de calendario completa.
    """
    print("  -> Iniciando la creación de dim_calendar (Dinámica)...")
    
    # 1. Recolectar y consolidar todas las fechas relevantes de las tablas de hechos y dimensiones
    all_dates = pd.concat([
        pd.to_datetime(data['sales_order']['order_date'], errors='coerce'),
        pd.to_datetime(data['web_session']['started_at'], errors='coerce'),
        pd.to_datetime(data['nps_response']['responded_at'], errors='coerce'),
        pd.to_datetime(data['payment']['paid_at'], errors='coerce'),
        pd.to_datetime(data['shipment']['shipped_at'], errors='coerce'),
        pd.to_datetime(data['shipment']['delivered_at'], errors='coerce'),
        pd.to_datetime(data['customer']['created_at'], errors='coerce'),
        pd.to_datetime(data['address']['created_at'], errors='coerce'),
        pd.to_datetime(data['product']['created_at'], errors='coerce'),
    ]).dropna()
    
    all_dates_normalized = all_dates.dt.normalize()

    if all_dates_normalized.empty:
        print("  Advertencia: No se encontraron fechas válidas para construir dim_calendar. Se devuelve un DF vacío.")
        # Devuelve un DataFrame vacío con la estructura esperada
        return pd.DataFrame(columns=['id', 'date', 'day', 'month', 'year', 'day_name', 'month_name', 'quarter', 'week_number', 'year_month', 'is_weekend'])

    # 2. Determinar el rango dinámico (mínima a máxima fecha) y generar el rango diario
    min_date = all_dates_normalized.min()
    max_date = all_dates_normalized.max()
    print(f"  Rango de fechas detectado: {min_date.strftime('%Y-%m-%d')} a {max_date.strftime('%Y-%m-%d')}")
    
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    df_calendar = pd.DataFrame(date_range, columns=['date'])
    
    # 3. Enriquecer con atributos de fecha (Jerarquía temporal)
    df_calendar['day'] = df_calendar['date'].dt.day
    df_calendar['month'] = df_calendar['date'].dt.month
    df_calendar['year'] = df_calendar['date'].dt.year
    df_calendar['day_name'] = df_calendar['date'].dt.day_name()
    df_calendar['month_name'] = df_calendar['date'].dt.month_name()
    df_calendar['quarter'] = df_calendar['date'].dt.quarter
    df_calendar['week_number'] = df_calendar['date'].dt.isocalendar().week.astype(int)
    df_calendar['year_month'] = df_calendar['date'].dt.strftime('%Y-%m')
    df_calendar['is_weekend'] = df_calendar['day_name'].isin(['Saturday', 'Sunday'])
    
    # 4. Crear Surrogate Key (SK) - Clave Sustituta
    df_calendar.reset_index(drop=True, inplace=True)
    df_calendar['id'] = df_calendar.index + 1
    
    # 5. Ordenar las columnas para el estándar dimensional (SK, NK, Atributos)
    cols = ['id'] + [col for col in df_calendar if col != 'id']
    df_calendar = df_calendar[cols]
    
    print("  -> dim_calendar creada exitosamente.")
    return df_calendar

def create_dim_customer(data):
    """
    Crea la Dimensión de Cliente (dim_customer).
    
    Consume:
        data['customer']
        
    Estructura de la Dimensión:
        - id (SK): Surrogate Key.
        - customer_key (NK): Natural Key (Original: 'customer_id').
        - Atributos del cliente.
    """
    print("  -> Creando dim_customer...")
    df = data['customer'].copy()
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'customer_id': 'customer_key'})
    
    # 2. Seleccionar columnas relevantes para la dimensión
    df = df[['customer_key', 'email', 'first_name', 'last_name', 'phone', 'status', 'created_at']]
    
    # 3. Crear Surrogate Key (SK) - Basada en la posición ordenada
    df = df.sort_values(by='customer_key')
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1
    
    # 4. Ordenar las columnas (SK, NK, Atributos)
    cols = ['id', 'customer_key'] + [col for col in df if col not in ['id', 'customer_key']]
    df = df[cols]
    
    print("  -> dim_customer creada.")
    return df

def create_dim_channel(data):
    """
    Crea la Dimensión de Canal (dim_channel).
    
    Consume:
        data['channel']
        
    Estructura de la Dimensión:
        - id (SK): Surrogate Key.
        - channel_key (NK): Natural Key (Original: 'channel_id').
        - Atributos del canal.
    """
    print("  -> Creando dim_channel...")
    df = data['channel'].copy()
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'channel_id': 'channel_key'})
    
    # 2. Crear Surrogate Key (SK)
    df = df.sort_values(by='channel_key')
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1
    
    # 3. Ordenar y seleccionar columnas (SK, NK, Atributos)
    cols = ['id', 'channel_key', 'code', 'name']
    df = df[cols]
    
    print("  -> dim_channel creada.")
    return df

def create_dim_address(data):
    """
    Crea la Dimensión de Dirección (dim_address).
    
    Realiza la desnormalización de la información de provincia (buscando 'province_name' y 'province_code').
    
    Consume:
        data['address']
        data['province']
        
    Estructura de la Dimensión:
        - id (SK): Surrogate Key.
        - address_key (NK): Natural Key (Original: 'address_id').
        - Atributos de la dirección.
    """
    print("  -> Creando dim_address (Desnormalizando provincia)...")
    address = data['address'].copy()
    province = data['province'].copy()
    
    # --- MERGE (LEFT JOIN) ---
    # Enriquecer la dirección con los detalles de la provincia.
    df = pd.merge(address, province, on='province_id', how='left')
    
    # 1. Renombrar claves y columnas desnormalizadas
    df = df.rename(columns={
        'address_id': 'address_key',
        'name': 'province_name',
        'code': 'province_code'
    })
    
    # 2. Seleccionar atributos finales para la dimensión
    df = df[['address_key', 'line1', 'line2', 'city', 'province_name', 
             'province_code', 'postal_code', 'country_code', 'created_at']]

    # 3. Crear Surrogate Key (SK)
    df = df.sort_values(by='address_key')
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1
    
    # 4. Ordenar las columnas (SK, NK, Atributos)
    cols = ['id', 'address_key'] + [col for col in df if col not in ['id', 'address_key']]
    df = df[cols]
    
    print("  -> dim_address creada.")
    return df

def create_dim_product(data):
    """
    Crea la Dimensión de Producto (dim_product).
    
    Realiza la desnormalización de la jerarquía de categorías (Categoría y Categoría Padre).
    
    Consume:
        data['product']
        data['product_category']
        
    Estructura de la Dimensión:
        - id (SK): Surrogate Key.
        - product_key (NK): Natural Key (Original: 'product_id').
        - Atributos del producto y su jerarquía.
    """
    print("  -> Creando dim_product (Desnormalizando categorías)...")
    product = data['product'].copy()
    category = data['product_category'].copy()

    # 1. Asegurar tipos de datos de claves para un join correcto
    product['category_id'] = product['category_id'].astype(str)
    category['category_id'] = category['category_id'].astype(str)
    category['parent_id'] = category['parent_id'].astype(str)
    
    # 2. Preparar categorías padre para el self-join
    parent_cats = category[['category_id', 'name']].rename(
        columns={'category_id': 'parent_id', 'name': 'parent_category_name'}
    )
    
    # --- MERGE 1 (Self-Join) ---
    # Enriquecer categorías con el nombre de su padre.
    categories_enriched = pd.merge(
        category,
        parent_cats,
        on='parent_id',
        how='left',
        suffixes=('_cat', '_parent')
    )

    # --- MERGE 2 ---
    # Unir la tabla de productos con las categorías ya enriquecidas.
    df = pd.merge(
        product,
        categories_enriched,
        on='category_id',
        how='left',
        suffixes=('_prod', '') 
    )
    
    # 3. Renombrar claves y resolver ambigüedades de nombres
    df = df.rename(columns={
        'product_id': 'product_key',
        'name_prod': 'name',      # Nombre del producto
        'name': 'category_name'   # Nombre de la categoría
    })
    
    # 4. Seleccionar atributos y limpiar valores nulos de jerarquía
    df = df[['product_key', 'sku', 'name', 'list_price', 'status', 
             'created_at', 'category_name', 'parent_category_name']]
    
    df['category_name'] = df['category_name'].fillna('Sin Categoría')
    df['parent_category_name'] = df['parent_category_name'].fillna('Sin Categoría')

    # 5. Crear la Surrogate Key (SK)
    df = df.sort_values(by='product_key')
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1

    # 6. Reordenar columnas (SK, NK, Atributos)
    cols = ['id', 'product_key'] + [col for col in df if col not in ['id', 'product_key']]
    df = df[cols]

    print("  -> dim_product creada.")
    return df

def create_dim_store(data):
    """
    Crea la Dimensión de Tienda (dim_store).
    
    Realiza la desnormalización de la dirección y provincia de la tienda (Doble JOIN).
    
    Consume:
        data['store']
        data['address']
        data['province']
        
    Estructura de la Dimensión:
        - id (SK): Surrogate Key.
        - store_key (NK): Natural Key (Original: 'store_id').
        - Atributos de la tienda, dirección y provincia.
    """
    print("  -> Creando dim_store (Desnormalizando dirección y provincia)...")
    store = data['store'].copy()
    address = data['address'].copy()
    province = data['province'].copy()
    
    # --- MERGE 1 ---
    # Unir la tabla de tiendas con la tabla de direcciones.
    store_addr = pd.merge(store, address, on='address_id', how='left')
    
    # --- MERGE 2 ---
    # Unir el resultado con la tabla de provincias.
    df = pd.merge(store_addr, province, on='province_id', how='left')
    
    # 1. Renombrar claves y resolver ambigüedades de nombres
    df = df.rename(columns={
        'store_id': 'store_key',
        'name_x': 'name',         # Nombre de la tienda
        'line1': 'line',          # Renombrar 'line1' a 'line'
        'name_y': 'province_name',
        'code': 'province_code'
    })
    
    # 2. Seleccionar atributos finales (excluyendo 'line2' de la dirección)
    df = df[['store_key', 'name', 'line', 'city', 'province_name', 
             'province_code', 'postal_code', 'country_code', 'created_at']]

    # 3. Crear Surrogate Key (SK)
    df = df.sort_values(by='store_key')
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1
    
    # 4. Ordenar las columnas (SK, NK, Atributos)
    cols = ['id', 'store_key'] + [col for col in df if col not in ['id', 'store_key']]
    df = df[cols]
    
    print("  -> dim_store creada.")
    return df

# --- Funciones de Creación de Hechos ---

def create_fact_sales_order(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Órdenes de Venta (fact_sales_order).
    Granularidad: Cabecera de la Orden.
    
    Consume:
        data['sales_order']
        dim_calendar (para la búsqueda de Foreign Keys de fecha/tiempo)
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'order_id')
        - order_date_id (FK): Foreign Key a dim_calendar
        - Claves Naturales para joins en el BI (customer_id, channel_id, store_id, etc.)
        - Métricas/Medidas (subtotal, tax_amount, total_amount)
    """
    print("  -> Creando fact_sales_order...")
    df = data['sales_order'].copy()
        
    # 1. Renombrar la Clave Natural (NK) y columnas de interés
    df = df.rename(columns={'order_id': 'id', 'status': 'status_order'})
    
    # 2. Buscar Foreign Keys (FKs) de dim_calendar para la fecha y extraer la hora
    df['order_date_id'] = _get_date_id(df['order_date'], dim_calendar)
    df['order_time'] = _get_time(df['order_date'])
    
    # 3. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['billing_address_id'] = df['billing_address_id'].fillna(-1).astype(int)
    df['shipping_address_id'] = df['shipping_address_id'].fillna(-1).astype(int)

    # 4. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'customer_id', 'channel_id', 'store_id', 'order_date_id', 'order_time',
        'billing_address_id', 'shipping_address_id', 'status_order', 'currency_code',
        'subtotal', 'tax_amount', 'shipping_fee', 'total_amount'
    ]
    print("  -> fact_sales_order creada.")
    return df[cols]

def create_fact_sales_order_item(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Ítems de Venta (fact_sales_order_item).
    Granularidad: Línea de Producto (Transaccional).
    
    Desnormaliza las claves de la cabecera de la orden (customer, channel, store, order_date).
    
    Consume:
        data['sales_order_item']
        data['sales_order']
        dim_calendar
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'order_item_id')
        - order_date_id (FK): Foreign Key a dim_calendar
        - Claves Naturales para joins en el BI (order_id, customer_id, product_id, etc.)
        - Métricas/Medidas (quantity, unit_price, line_total)
    """
    print("  -> Creando fact_sales_order_item...")
    items = data['sales_order_item'].copy()
    # Seleccionar solo las claves necesarias de la cabecera
    orders = data['sales_order'][['order_id', 'customer_id', 'channel_id', 'store_id', 'order_date']]
    
    # --- MERGE (LEFT JOIN) ---
    # Denormalizar los ítems con las claves de la cabecera de la orden
    df = pd.merge(items, orders, on='order_id', how='left')
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'order_item_id': 'id'})
    
    # 2. Buscar Foreign Key (FK) de dim_calendar
    df['order_date_id'] = _get_date_id(df['order_date'], dim_calendar)
    
    # 3. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['product_id'] = df['product_id'].fillna(-1).astype(int)

    # 4. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'order_id', 'customer_id', 'channel_id', 'store_id', 'product_id', 'order_date_id',
        'quantity', 'unit_price', 'discount_amount', 'line_total'
    ]
    print("  -> fact_sales_order_item creada.")
    return df[cols]

def create_fact_payment(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Pagos (fact_payment).
    Granularidad: Transacción de Pago.
    
    Denormaliza las claves de la orden (customer, address, channel, store).
    
    Consume:
        data['payment']
        data['sales_order']
        dim_calendar
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'payment_id')
        - paid_at_date_id (FK): Foreign Key a dim_calendar
        - Claves Naturales para joins en el BI (customer_id, channel_id, store_id, etc.)
        - Métricas/Medidas (amount)
    """
    print("  -> Creando fact_payment...")
    payments = data['payment'].copy()
    # Seleccionar solo las claves necesarias de la cabecera
    orders = data['sales_order'][['order_id', 'customer_id', 'billing_address_id', 'channel_id', 'store_id']]
    
    # --- MERGE (LEFT JOIN) ---
    # Denormalizar los pagos con las claves de la cabecera de la orden.
    df = pd.merge(payments, orders, on='order_id', how='left')
    
    # 1. Renombrar la Clave Natural (NK) y columnas de interés
    df = df.rename(columns={'payment_id': 'id', 'status': 'status_payment'})
    
    # 2. Buscar Foreign Keys (FKs) de dim_calendar y extraer la hora
    df['paid_at_date_id'] = _get_date_id(df['paid_at'], dim_calendar)
    df['paid_at_time'] = _get_time(df['paid_at'])
    
    # 3. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['billing_address_id'] = df['billing_address_id'].fillna(-1).astype(int)

    # 4. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'customer_id', 'billing_address_id', 'channel_id', 'store_id',
        'method', 'status_payment', 'amount', 'paid_at_date_id', 'paid_at_time',
        'transaction_ref'
    ]
    print("  -> fact_payment creada.")
    return df[cols]

def create_fact_shipment(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Envíos (fact_shipment).
    Granularidad: Envío.
    
    Calcula la métrica derivada 'dias_de_entrega'.
    Denormaliza las claves de la orden (customer, shipping_address, channel).
    
    Consume:
        data['shipment']
        data['sales_order']
        dim_calendar
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'shipment_id')
        - shipped_at_date_id / delivered_at_date_id (FKs): Foreign Keys a dim_calendar
        - Métrica/Medida (dias_de_entrega)
    """
    print("  -> Creando fact_shipment...")
    shipments = data['shipment'].copy()
    # Seleccionar solo las claves necesarias de la cabecera
    orders = data['sales_order'][['order_id', 'customer_id', 'shipping_address_id', 'channel_id']]
    
    # --- MERGE (LEFT JOIN) ---
    # Denormalizar los envíos con las claves de la cabecera de la orden.
    df = pd.merge(shipments, orders, on='order_id', how='left')
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'shipment_id': 'id'})
    
    # 2. Convertir a datetime ANTES de calcular métricas o buscar FKs
    df['shipped_at'] = pd.to_datetime(df['shipped_at'], errors='coerce')
    df['delivered_at'] = pd.to_datetime(df['delivered_at'], errors='coerce')

    # 3. Buscar Foreign Keys (FKs) de dim_calendar
    df['shipped_at_date_id'] = _get_date_id(df['shipped_at'], dim_calendar)
    df['delivered_at_date_id'] = _get_date_id(df['delivered_at'], dim_calendar)
    
    # 4. Extraer tiempos
    df['shipped_at_time'] = _get_time(df['shipped_at'])
    df['delivered_at_time'] = _get_time(df['delivered_at'])

    # 5. Calcular métricas: Diferencia en días entre envío y entrega
    df['dias_de_entrega'] = (df['delivered_at'] - df['shipped_at']).dt.days

    # 6. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['shipping_address_id'] = df['shipping_address_id'].fillna(-1).astype(int)

    # 7. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'customer_id', 'shipping_address_id', 'channel_id', 'carrier',
        'shipped_at_date_id', 'shipped_at_time',
        'delivered_at_date_id', 'delivered_at_time', 'tracking_number', 
        'dias_de_entrega'
    ]
    print("  -> fact_shipment creada.")
    return df[cols]

def create_fact_web_session(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Sesiones Web (fact_web_session).
    Granularidad: Sesión Web.
    
    Consume:
        data['web_session']
        dim_calendar
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'session_id')
        - started_at_date_id / ended_at_date_id (FKs): Foreign Keys a dim_calendar
        - Claves Naturales para joins en el BI (customer_id)
        - Atributos (source, device)
    """
    print("  -> Creando fact_web_session...")
    df = data['web_session'].copy()
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'session_id': 'id'})
    
    # 2. Buscar Foreign Keys (FKs) de dim_calendar
    df['started_at_date_id'] = _get_date_id(df['started_at'], dim_calendar)
    df['ended_at_date_id'] = _get_date_id(df['ended_at'], dim_calendar)
    
    # 3. Extraer tiempos
    df['started_at_time'] = _get_time(df['started_at'])
    df['ended_at_time'] = _get_time(df['ended_at'])
    
    # 4. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)

    # 5. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'customer_id', 'started_at_date_id', 'started_at_time',
        'ended_at_date_id', 'ended_at_time', 'source', 'device'
    ]
    print("  -> fact_web_session creada.")
    return df[cols]

def create_fact_nps_response(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Respuestas NPS (fact_nps_response).
    Granularidad: Respuesta NPS.
    
    Consume:
        data['nps_response']
        dim_calendar
        
    Estructura de la Tabla de Hechos:
        - id (NK): Clave Natural (Original: 'nps_id')
        - responded_at_date_id (FK): Foreign Key a dim_calendar
        - Claves Naturales para joins en el BI (customer_id, channel_id)
        - Métrica/Medida (score)
    """
    print("  -> Creando fact_nps_response...")
    df = data['nps_response'].copy()
    
    # 1. Renombrar la Clave Natural (NK)
    df = df.rename(columns={'nps_id': 'id'})
    
    # 2. Buscar Foreign Keys (FKs) de dim_calendar y extraer la hora
    df['responded_at_date_id'] = _get_date_id(df['responded_at'], dim_calendar)
    df['responded_at_time'] = _get_time(df['responded_at'])
    
    # 3. Limpiar claves naturales (establecer -1 para "Desconocido/N/A")
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)

    # 4. Seleccionar y ordenar columnas finales
    cols = [
        'id', 'customer_id', 'channel_id', 'responded_at_date_id',
        'responded_at_time', 'score'
    ]
    print("  -> fact_nps_response creada.")
    return df[cols]

# --- Función Orquestadora del Proceso de Transformación ---

def transform_all_data(data):
    """
    Orquesta la creación de todas las Dimensiones y Tablas de Hechos del Data Warehouse.
    
    El proceso sigue el orden de creación: Dimensiones -> Tablas de Hechos.
    La dimensión de calendario (dim_calendar) se crea primero y se reutiliza para
    generar las Foreign Keys (FKs) de fecha en todas las Tablas de Hechos.
    
    Args:
        data (dict): Un diccionario donde cada clave es el nombre
                     de una tabla cruda (ej: 'customer') y el valor
                     es un DataFrame de pandas con esos datos.
                     
    Returns:
        dict: Un diccionario con los DataFrames transformados del DW.
              Las claves son los nombres de las tablas del DW (ej: 'dim_customer', 'fact_sales_order').
    """
    if data is None:
        print("Error en transform_all_data: El diccionario de datos de entrada es None. Abortando transformación.")
        return None
        
    print("\nIniciando Proceso de Transformación (T) a Modelo Dimensional...")
    
    dw_tables = {}
    
    # --- Paso 1: Creación y Enriquecimiento de Dimensiones ---
    print("\n--- [FASE 1: DIMENSIONES] ---")
    
    # La dimensión de calendario es crucial y se crea primero, de forma dinámica.
    dw_tables['dim_calendar'] = create_dim_calendar(data=data)
    
    # El resto de dimensiones (Cliente, Producto, etc.)
    dw_tables['dim_customer'] = create_dim_customer(data)
    dw_tables['dim_product'] = create_dim_product(data)
    dw_tables['dim_channel'] = create_dim_channel(data)
    dw_tables['dim_address'] = create_dim_address(data)
    dw_tables['dim_store'] = create_dim_store(data)
    
    # --- Paso 2: Creación y Enriquecimiento de Tablas de Hechos ---
    print("\n--- [FASE 2: TABLAS DE HECHOS] ---")
    # Almacenar dim_calendar para pasarla a todas las funciones de hechos
    dim_calendar = dw_tables['dim_calendar'] 
    
    # Creación de Tablas de Hechos (transaccionales y de eventos)
    dw_tables['fact_sales_order'] = create_fact_sales_order(data, dim_calendar)
    dw_tables['fact_sales_order_item'] = create_fact_sales_order_item(data, dim_calendar)
    dw_tables['fact_payment'] = create_fact_payment(data, dim_calendar)
    dw_tables['fact_shipment'] = create_fact_shipment(data, dim_calendar)
    dw_tables['fact_web_session'] = create_fact_web_session(data, dim_calendar)
    dw_tables['fact_nps_response'] = create_fact_nps_response(data, dim_calendar)
    
    print("\nProceso de Transformación (T) completado. Tablas DW generadas.")
    return dw_tables

if __name__ == '_main_':
    print("Este módulo (transform.py) contiene las funciones de transformación (T).")
    print("No está diseñado para ejecutarse directamente como script principal.")
    print("Impórtalo y llama a la función 'transform_all_data(data)' desde tu script ETL principal.")