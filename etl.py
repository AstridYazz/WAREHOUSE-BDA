import os
import pandas as pd
import numpy as np
import gc
from sqlalchemy import create_engine


#  Ruta del archivo original
csv_path = './data/airline_2m.csv'

#  Archivo de salida comprimido
output_file = 'airline_cleaned.csv.gz'

#  Tamaño del lote optimizado
chunk_size = 50000  

#  Definir tipos de datos para reducir consumo de memoria (evita float16 en `read_csv`)
dtypes = {
    'Year': 'int16',
    'Quarter': 'int8',
    'Month': 'int8',
    'DayofMonth': 'int8',
    'DayOfWeek': 'int8',
    'Flight_Number_Reporting_Airline': 'int32',
    'OriginAirportID': 'int32',
    'DestAirportID': 'int32',
    'CRSDepTime': 'int16',
    'DepTime': 'float32', 
    'DepDelayMinutes': 'float32',
    'ArrDelayMinutes': 'float32',
    'Cancelled': 'int8',
    'Diverted': 'int8',
    'Distance': 'float32',
    'CarrierDelay': 'float32',
    'WeatherDelay': 'float32',
    'NASDelay': 'float32',
    'SecurityDelay': 'float32',
    'LateAircraftDelay': 'float32',
}

#  Columnas clave a mantener
columnas_clave = ['FlightDate', 'DepTime', 'ArrTime', 'Origin', 'Dest', 'Reporting_Airline']

#  Columnas numéricas que no deben tener valores negativos
columnas_sin_negativos = ['DepDelayMinutes', 'ArrDelayMinutes', 'TaxiOut', 'TaxiIn', 'CRSElapsedTime', 'ActualElapsedTime']

#   PROCESAMIENTO EN LOTES con verificación de archivo
try:
    if os.path.exists(output_file):
        print(" El archivo limpio ya existe. Se eliminará antes de regenerarlo.")
        os.remove(output_file)

    with pd.read_csv(csv_path, chunksize=chunk_size, delimiter=',', dtype=dtypes, encoding="ISO-8859-1", low_memory=True) as reader:
        first_chunk = True  #  Para escribir encabezados solo en el primer lote
        for chunk in reader:
            print(f" Procesando lote con {len(chunk)} registros...")

            #  ELIMINAR COLUMNAS INNECESARIAS
            columnas_innecesarias = ['Unnamed: 0']
            chunk.drop(columns=[col for col in columnas_innecesarias if col in chunk.columns], inplace=True)

            #  VERIFICAR QUE LAS COLUMNAS CLAVE EXISTEN
            columnas_presentes = [col for col in columnas_clave if col in chunk.columns]
            if columnas_presentes:
                chunk.dropna(subset=columnas_presentes, how="any", inplace=True)

            #  CONVERTIR `FlightDate` A `datetime`
            if 'FlightDate' in chunk.columns:
                chunk['FlightDate'] = pd.to_datetime(chunk['FlightDate'], errors='coerce')

            #  ELIMINAR FILAS CON VALORES NEGATIVOS
            for col in columnas_sin_negativos:
                if col in chunk.columns:
                    chunk = chunk[chunk[col] >= 0]

            #  NORMALIZAR FORMATOS DE TEXTO
            columnas_texto = ['Reporting_Airline', 'Origin', 'Dest']
            for col in columnas_texto:
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype("category")  # ⚡ Reduce memoria

            #  CALCULOS ADICIONALES
            if 'DepTime' in chunk.columns and 'ArrTime' in chunk.columns:
                chunk['DuracionVuelo'] = chunk['ArrTime'] - chunk['DepTime']
            
            if 'DepDelayMinutes' in chunk.columns:
                chunk['RetrasoImportante'] = np.where(chunk['DepDelayMinutes'] > 15, 1, 0)

            #  GUARDAR LOTE EN CSV COMPRIMIDO
            chunk.to_csv(output_file, mode='a', index=False, header=first_chunk, compression='gzip')
            first_chunk = False  

            print("  Lote guardado con éxito.")

    print("  Limpieza completa. Archivo final guardado como airline_cleaned.csv.gz")

except Exception as e:
    print(f"  Error en la ejecución: {e}")


# 🔹 **VERIFICAR SI EL ARCHIVO SE GENERÓ**
if os.path.exists(output_file):
    print("  El archivo airline_cleaned.csv.gz se generó correctamente.")
else:
    print("  ERROR: El archivo airline_cleaned.csv.gz NO se generó correctamente.")

#  Intentar cargar los datos limpios en pequeños lotes
try:
    print(" Cargando los datos limpios en pequeños lotes para verificar integridad...")
    sample_size = 1000  # Solo leer una parte para verificar
    df_sample = pd.read_csv(output_file, compression='gzip', low_memory=False, nrows=sample_size)
    print("  Se cargó una muestra de los datos limpiados correctamente.")
    print(df_sample.head())

    #   **Eliminar la muestra después de verificar**
    del df_sample
    gc.collect()  # Forzar la liberación de memoria
    print(" Memoria liberada después de la verificación.")

except Exception as e:
    print(f"  Error al leer el archivo limpio: {e}")

#  Cargar datos limpios
df = pd.read_csv(output_file, compression='gzip', low_memory=False)

#  FACT_VUELOS (Hechos de Vuelos)

fact_vuelos = df[['FlightDate', 'OriginAirportID', 'DestAirportID', 'Reporting_Airline', 'Distance',
                   'CRSDepTime', 'DepTime', 'CRSArrTime', 'ArrTime', 'CRSElapsedTime', 'ActualElapsedTime',
                   'Cancelled', 'Diverted', 'TaxiOut', 'TaxiIn']].copy()

# Renombrar columnas según el modelo dimensional
fact_vuelos.rename(columns={
    'FlightDate': 'FechaVuelo',
    'OriginAirportID': 'ID_Aeropuerto_Origen',
    'DestAirportID': 'ID_Aeropuerto_Destino',
    'Reporting_Airline': 'ID_Aerolinea',
    'Distance': 'Distancia',
    'CRSDepTime': 'HoraSalidaProgramada',
    'DepTime': 'HoraSalidaReal',
    'CRSArrTime': 'HoraLlegadaProgramada',
    'ArrTime': 'HoraLlegadaReal',
    'CRSElapsedTime': 'DuracionPlanificada',
    'ActualElapsedTime': 'DuracionReal',
    'Cancelled': 'Cancelado',
    'Diverted': 'Desviado',
    'TaxiOut': 'TaxiOut',
    'TaxiIn': 'TaxiIn'
}, inplace=True)

# Generar ID_Vuelo como clave primaria
fact_vuelos['ID_Vuelo'] = range(1, len(fact_vuelos) + 1)

# Agregar la clave foránea ID_Tiempo basada en la FechaVuelo
fact_vuelos['ID_Tiempo'] = fact_vuelos['FechaVuelo']

# Calcular RetrasoFlag (1 = más de 15 min, 0 = menor a 15 min)
fact_vuelos['RetrasoFlag'] = (df['DepDelayMinutes'] > 15).astype(int)

# Asignar ID_Retraso según la existencia de retraso
fact_vuelos['ID_Retraso'] = np.where(df['DepDelayMinutes'] > 0, fact_vuelos['ID_Vuelo'], np.nan)

# Asignar ID_Cancelacion si el vuelo fue cancelado
fact_vuelos['ID_Cancelacion'] = np.where(df['Cancelled'] == 1, fact_vuelos['ID_Vuelo'], np.nan)

# Asignar ID_Desviacion si el vuelo fue desviado
fact_vuelos['ID_Desviacion'] = np.where(df['Diverted'] == 1, fact_vuelos['ID_Vuelo'], np.nan)

# Guardar Tablas en formato CSV
tablas = {
    "fact_vuelos.csv": fact_vuelos,
}


#   FACT_OPERACIONES_AEROPORTUARIAS (Actividad Aeroportuaria Mejorada)
fact_operaciones_aeropuertos = df.groupby(['OriginAirportID', 'FlightDate']).agg(
    Vuelos_Atendidos=('FlightDate', 'count'),
    Vuelos_Cancelados=('Cancelled', 'sum'),
    Retrasos_Promedio_Salida=('DepDelayMinutes', 'mean'),
    Retrasos_Promedio_Llegada=('ArrDelayMinutes', 'mean'),
    Total_TaxiOut=('TaxiOut', 'sum'),
    Total_TaxiIn=('TaxiIn', 'sum'),
    Retrasos_Aerolinea=('CarrierDelay', 'sum'),
    Retrasos_Clima=('WeatherDelay', 'sum'),
    Retrasos_Trafico_Aereo=('NASDelay', 'sum'),
    Retrasos_Seguridad=('SecurityDelay', 'sum'),
    Retrasos_Llegada_Tardia_Avion=('LateAircraftDelay', 'sum'),
    Retrasos_Severos=('DepDelayMinutes', lambda x: (pd.to_numeric(x, errors='coerce').fillna(0) > 60).sum())  
).reset_index()

# Calcular el promedio general de retrasos combinando salida y llegada
fact_operaciones_aeropuertos['Retrasos_Promedio_Total'] = fact_operaciones_aeropuertos[['Retrasos_Promedio_Salida', 'Retrasos_Promedio_Llegada']].mean(axis=1)

# Renombrar columnas según el modelo dimensional
fact_operaciones_aeropuertos.rename(columns={
    'OriginAirportID': 'ID_Aeropuerto',
    'FlightDate': 'ID_Tiempo',
}, inplace=True)

# Generar ID_Operacion como clave primaria
fact_operaciones_aeropuertos['ID_Operacion'] = range(1, len(fact_operaciones_aeropuertos) + 1)

# Guardar Tablas en formato CSV
tablas = {
    "fact_operaciones_aeropuertos.csv": fact_operaciones_aeropuertos,
}



#  DIM_AEROLINEA
dim_aerolinea = df[['Reporting_Airline', 'DOT_ID_Reporting_Airline']].drop_duplicates().copy()

# Generar ID_Aerolinea como clave primaria
dim_aerolinea['ID_Aerolinea'] = range(1, len(dim_aerolinea) + 1)

# Renombrar columnas según el modelo dimensional
dim_aerolinea.rename(columns={
    'Reporting_Airline': 'Codigo_IATA',
    'DOT_ID_Reporting_Airline': 'DOT_ID'
}, inplace=True)

# Calcular promedio de cancelaciones por aerolínea
promedio_cancelaciones = df.groupby('Reporting_Airline')['Cancelled'].mean().reset_index()
promedio_cancelaciones.rename(columns={'Cancelled': 'Promedio_Cancelaciones'}, inplace=True)

# Calcular tiempo promedio de retraso por aerolínea
tiempo_retraso = df.groupby('Reporting_Airline')[['DepDelayMinutes', 'ArrDelayMinutes']].mean().reset_index()
tiempo_retraso['Tiempo_Promedio_Retraso'] = tiempo_retraso[['DepDelayMinutes', 'ArrDelayMinutes']].mean(axis=1)

# Unir cálculos con la tabla de dimensiones
dim_aerolinea = dim_aerolinea.merge(promedio_cancelaciones, left_on='Codigo_IATA', right_on='Reporting_Airline', how='left')
dim_aerolinea = dim_aerolinea.merge(tiempo_retraso[['Reporting_Airline', 'Tiempo_Promedio_Retraso']], left_on='Codigo_IATA', right_on='Reporting_Airline', how='left')

# Eliminar columnas temporales usadas en el merge
dim_aerolinea.drop(columns=['Codigo_IATA'], inplace=True)

# Guardar Tablas en formato CSV
tablas = {
    "dim_aerolinea.csv": dim_aerolinea,
}



#  DIM_AEROPUERTO (Dimensión de Aeropuertos)
dim_aeropuerto = df[['OriginAirportID', 'Origin', 'OriginCityName', 'OriginState', 'OriginWac']].drop_duplicates().copy()

# Generar ID_Aeropuerto como clave primaria
dim_aeropuerto['ID_Aeropuerto'] = range(1, len(dim_aeropuerto) + 1)

# Renombrar columnas según el modelo dimensional
dim_aeropuerto.rename(columns={
    'OriginAirportID': 'Codigo_Aeropuerto',
    'Origin': 'Nombre_Aeropuerto',
    'OriginCityName': 'Ciudad',
    'OriginState': 'Estado',
    'OriginWac': 'WAC_Code'
}, inplace=True)

# Cálculo de Cantidad de Vuelos Diarios
vuelos_por_aeropuerto = df.groupby('OriginAirportID').size().reset_index(name='Cantidad_Vuelos_Diarios')

# Cálculo del Promedio de Retrasos
retraso_prom_aeropuerto = df.groupby('OriginAirportID')[['DepDelayMinutes', 'ArrDelayMinutes']].mean().reset_index()
retraso_prom_aeropuerto['Promedio_Retrasos'] = retraso_prom_aeropuerto[['DepDelayMinutes', 'ArrDelayMinutes']].mean(axis=1)

# Cálculo del Promedio de Cancelaciones
cancelaciones_por_aeropuerto = df.groupby('OriginAirportID')['Cancelled'].mean().reset_index()
cancelaciones_por_aeropuerto.rename(columns={'Cancelled': 'Promedio_Cancelaciones'}, inplace=True)

# Unir los cálculos con la tabla de aeropuertos
dim_aeropuerto = dim_aeropuerto.merge(vuelos_por_aeropuerto, left_on='Codigo_Aeropuerto', right_on='OriginAirportID', how='left')
dim_aeropuerto = dim_aeropuerto.merge(retraso_prom_aeropuerto[['OriginAirportID', 'Promedio_Retrasos']], left_on='Codigo_Aeropuerto', right_on='OriginAirportID', how='left')
dim_aeropuerto = dim_aeropuerto.merge(cancelaciones_por_aeropuerto, left_on='Codigo_Aeropuerto', right_on='OriginAirportID', how='left')

# Eliminar columnas temporales
dim_aeropuerto.drop(columns=['OriginAirportID'], inplace=True)

# Guardar Tablas en formato CSV
tablas = {
    "dim_aeropuerto.csv": dim_aeropuerto,
}



#  DIM_TIEMPO
df['FlightDate'] = pd.to_datetime(df['FlightDate'], errors='coerce')
dim_tiempo = df[['FlightDate']].drop_duplicates().copy()

# Renombrar clave primaria
dim_tiempo.rename(columns={'FlightDate': 'ID_Tiempo'}, inplace=True)

# Extraer atributos de tiempo
dim_tiempo['Año'] = dim_tiempo['ID_Tiempo'].dt.year
dim_tiempo['Mes'] = dim_tiempo['ID_Tiempo'].dt.month
dim_tiempo['Día'] = dim_tiempo['ID_Tiempo'].dt.day
dim_tiempo['DíaDeLaSemana'] = dim_tiempo['ID_Tiempo'].dt.dayofweek + 1  # Ajuste para que lunes sea 1 y domingo 7
dim_tiempo['Trimestre'] = dim_tiempo['ID_Tiempo'].dt.quarter

# Calcular estación del año
dim_tiempo['Estación'] = dim_tiempo['Mes'].apply(lambda x: 'Invierno' if x in [12, 1, 2] else 
                                                            'Primavera' if x in [3, 4, 5] else 
                                                            'Verano' if x in [6, 7, 8] else 'Otoño')

# Guardar Tablas en formato CSV
tablas = {
    "dim_tiempo.csv": dim_tiempo,
}


#     DIM_CANCELACION
dim_cancelacion = df[['CancellationCode']].dropna().drop_duplicates().copy()

# Generar ID_Cancelacion
dim_cancelacion['ID_Cancelacion'] = range(1, len(dim_cancelacion) + 1)

# Mapeo de razones de cancelación según código
razones_cancelacion = {
    'A': 'Problemas con la aerolínea',
    'B': 'Condiciones climáticas',
    'C': 'Problemas del sistema nacional de aviación',
    'D': 'Otros',
    'Un': 'Desconocido'  # Se agrega el valor desconocido
}


# Aplicar mapeo de razones de cancelación
dim_cancelacion['Razón_Cancelacion'] = dim_cancelacion['CancellationCode'].map(razones_cancelacion)

# Reemplazar valores nulos con "Indefinido"
dim_cancelacion['Razón_Cancelacion'].fillna('Indefinido', inplace=True)


# Guardar Tablas en formato CSV
tablas = {
    "dim_cancelacion.csv": dim_cancelacion,
}


#     DIM_DESVIACIONES
dim_desviaciones = df[df['Diverted'] == 1][['Div1AirportID', 'ArrTime', 'CRSArrTime']].dropna().copy()

# Generar ID_Desviacion
dim_desviaciones['ID_Desviacion'] = range(1, len(dim_desviaciones) + 1)

# Calcular Retraso_Desviacion
dim_desviaciones['Retraso_Desviacion'] = dim_desviaciones['ArrTime'] - dim_desviaciones['CRSArrTime']

# Renombrar columnas según el modelo dimensional
dim_desviaciones.rename(columns={
    'Div1AirportID': 'ID_Aeropuerto_Desviado',
    'ArrTime': 'Hora_Nueva_Llegada'
}, inplace=True)


# Guardar Tablas en formato CSV
tablas = {
    "dim_desviaciones.csv": dim_desviaciones,
}



# DIM_RETRASO
dim_retraso = df[['CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay', 
                  'DepDelayMinutes', 'ArrDelayMinutes']].dropna().copy()

# Generar ID_Retraso
dim_retraso['ID_Retraso'] = range(1, len(dim_retraso) + 1)

# Renombrar columnas según el modelo dimensional
dim_retraso.rename(columns={
    'CarrierDelay': 'Retraso_Aerolinea',
    'WeatherDelay': 'Retraso_Clima',
    'NASDelay': 'Retraso_Trafico_Aereo',
    'SecurityDelay': 'Retraso_Seguridad',
    'LateAircraftDelay': 'Retraso_Llegada_Tardia_Avion',
    'DepDelayMinutes': 'Retraso_Salida',
    'ArrDelayMinutes': 'Retraso_Llegada'
}, inplace=True)

# Calcular el total de minutos de retraso (sumando todos los factores)
dim_retraso['Retraso_TotalMinutos'] = (
    dim_retraso['Retraso_Salida'] + 
    dim_retraso['Retraso_Llegada'] + 
    dim_retraso['Retraso_Aerolinea'] + 
    dim_retraso['Retraso_Clima'] + 
    dim_retraso['Retraso_Trafico_Aereo'] + 
    dim_retraso['Retraso_Seguridad']
)

# Categorizar los retrasos
dim_retraso['Retraso_Categoria'] = dim_retraso['Retraso_TotalMinutos'].apply(
    lambda x: 'Sin Retraso' if x == 0 else 
              'Retraso Leve' if x <= 15 else 
              'Retraso Moderado' if x <= 60 else 
              'Retraso Severo'
)


# Guardar Tablas en formato CSV
tablas = {
    "dim_retraso.csv": dim_retraso,
}




# 🔹 Guardar cada tabla en CSV, verificando que contenga datos antes de guardarla
for nombre_archivo, df in tablas.items():
    if not df.empty:
        df.to_csv(nombre_archivo, index=False)
        print(f"  {nombre_archivo} exportado correctamente.")
    else:
        print(f" {nombre_archivo} está vacío y no se guardó.")




#   Mostrar ejemplo de tabla de hechos
print("  Transformación completada. Todas las tablas han sido procesadas correctamente.")
print("\n🔹 Vista previa de FACT_VUELOS:")
print(fact_vuelos.head())  # Muestra las primeras filas en la terminal


#CARGAR DATOS EN POSTGRESQL

#  Configuración de la conexión a PostgreSQL
DB_USER = "user_bda"
DB_PASSWORD = "12345678"
DB_HOST = "localhost"  # Cambiar si es un servidor externo
DB_PORT = "5432"  # Puerto por defecto de PostgreSQL
DB_NAME = "proyectoBDA"

#   Crear conexión a PostgreSQL
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

#  Ruta del archivo limpio comprimido
file_path = "airline_cleaned.csv.gz"

#  Cargar en DataFrame por lotes para optimizar
chunk_size = 100000
for chunk in pd.read_csv(file_path, chunksize=chunk_size, compression='gzip'):
    print(f" Cargando lote con {len(chunk)} registros...")
    
    # Insertar en la tabla de vuelos
    chunk.to_sql("vuelos", engine, if_exists="append", index=False)
    
    print("  Lote cargado correctamente.")

print("  Carga completa en PostgreSQL.")
