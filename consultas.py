import pandas as pd
import psycopg2

#  Configuración de conexión a PostgreSQL
DB_USER = "user_bda"
DB_PASSWORD = "12345678"
DB_HOST = "localhost"  # Cambiar si es un servidor externo
DB_PORT = "5432"
DB_NAME = "DataWarehouse"

#  Establecer conexión
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)

#  Definir consultas corregidas
queries = {
    "Aerolínea con más vuelos": """
        SELECT "Reporting_Airline" AS aerolinea, 
               COUNT(*) AS total_vuelos,
               ROUND(CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vuelos) AS NUMERIC), 2) AS porcentaje
        FROM vuelos
        GROUP BY "Reporting_Airline"
        ORDER BY total_vuelos DESC
        LIMIT 5;
    """,
    "Retraso promedio por aeropuerto": """
        SELECT "OriginAirportID" AS Aeropuerto, 
               ROUND(CAST(AVG("ArrDelayMinutes") AS NUMERIC), 2) AS retraso_promedio
        FROM vuelos
        WHERE "ArrDelayMinutes" IS NOT NULL
        GROUP BY "OriginAirportID"
        ORDER BY retraso_promedio DESC
        LIMIT 10;
    """,
    "Porcentaje de vuelos cancelados por aerolínea": """
        SELECT "Reporting_Airline" AS aerolinea, 
               COUNT(*) AS total_vuelos, 
               SUM(CASE WHEN "Cancelled" = 1 THEN 1 ELSE 0 END) AS vuelos_cancelados,
               ROUND(CAST(SUM(CASE WHEN "Cancelled" = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS NUMERIC), 2) AS porcentaje_cancelado
        FROM vuelos
        GROUP BY "Reporting_Airline"
        ORDER BY porcentaje_cancelado DESC
        LIMIT 5;
    """,
    "Distribución de retrasos por hora": """
        SELECT 
            EXTRACT(HOUR FROM TO_TIMESTAMP(LPAD("DepTime"::TEXT, 4, '0'), 'HH24MI')) AS hora, 
            ROUND(AVG("DepDelayMinutes")::NUMERIC, 2) AS retraso_promedio,
            COUNT(*) AS cantidad_vuelos
        FROM vuelos
        WHERE "DepDelayMinutes" IS NOT NULL 
            AND "DepTime" IS NOT NULL
            AND "DepTime" BETWEEN 0 AND 2359  -- Asegurar que los valores son válidos
        GROUP BY hora
        ORDER BY hora;
    """,
    "Tiempo real vs. planeado en vuelos": """
        SELECT "Origin" AS origen, "Dest" AS destino, 
               ROUND(CAST(AVG("ActualElapsedTime") AS NUMERIC), 2) AS tiempo_real, 
               ROUND(CAST(AVG("CRSElapsedTime") AS NUMERIC), 2) AS tiempo_planificado,
               ROUND(CAST(AVG("ActualElapsedTime") - AVG("CRSElapsedTime") AS NUMERIC), 2) AS diferencia
        FROM vuelos
        WHERE "CRSElapsedTime" IS NOT NULL AND "ActualElapsedTime" IS NOT NULL
        GROUP BY "Origin", "Dest"
        ORDER BY diferencia DESC
        LIMIT 10;
    """
}

#  Ejecutar y mostrar resultados
with conn.cursor() as cursor:
    for name, query in queries.items():
        print(f"\n {name} ")
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        print(df)

#  Cerrar conexión
conn.close()
