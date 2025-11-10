# ============================================================
# 📥 Descarga automática de datos históricos de Open-Meteo
# Para la alcaldía Gustavo A. Madero (CDMX)
# Rango: 2024-01-01 → hasta ayer
# ============================================================

import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def descargar_datos_historicos(lat, lon, start_date, end_date, archivo_csv):
    """
    Descarga datos horarios de Open-Meteo para un rango de fechas
    y los guarda en un CSV con el formato del dataset histórico.
    """
    
    # === 1. Definir la URL base y las variables ===
    api_url = "https://archive-api.open-meteo.com/v1/archive"
    variables_horarias = [
        "temperature_2m",
        "relative_humidity_2m",
        "dewpoint_2m",
        "pressure_msl",
        "precipitation",
        "wind_speed_10m",
        "wind_gusts_10m",
        "wind_direction_10m",
        "weathercode"
    ]
    
    # === 2. Configurar los parámetros de la solicitud ===
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables_horarias),
        "timezone": "America/Mexico_City"
    }
    
    print(f"🚀 Solicitando datos desde {start_date} hasta {end_date}...")
    
    try:
        start_time = time.time()
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()
        end_time = time.time()
        
        print(f"✅ Datos recibidos en {end_time - start_time:.2f} segundos")
        
        # === 3. Convertir a DataFrame ===
        df = pd.DataFrame(data["hourly"])
        
        # === 4. Conversión de unidades ===
        df["wind_speed_10m"] = df["wind_speed_10m"] * 3.6
        df["wind_gusts_10m"] = df["wind_gusts_10m"] * 3.6
        
        # === 5. Renombrar columnas ===
        df = df.rename(columns={
            "time": "fecha_hora",
            "temperature_2m": "temperatura_C",
            "relative_humidity_2m": "humedad_%",
            "dewpoint_2m": "punto_rocio_C",
            "pressure_msl": "presion_hPa",
            "precipitation": "precipitacion_mm",
            "wind_speed_10m": "viento_velocidad_kmh",
            "wind_gusts_10m": "viento_rafaga_kmh",
            "wind_direction_10m": "viento_direccion_°",
            "weathercode": "codigo_clima_wmo"
        })
        
        # === 6. Reordenar columnas ===
        columnas_finales = [
            "fecha_hora", "temperatura_C", "humedad_%", "punto_rocio_C",
            "presion_hPa", "precipitacion_mm", "viento_velocidad_kmh",
            "viento_rafaga_kmh", "viento_direccion_°", "codigo_clima_wmo"
        ]
        df = df[columnas_finales]
        
        # === 7. Formatear fecha ===
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"]).dt.strftime("%Y-%m-%dT%H:%M")
        
        # === 8. Guardar CSV ===
        df.to_csv(archivo_csv, index=False, encoding="utf-8")
        
        print(f"💾 {len(df)} registros guardados en '{archivo_csv}'")
        print(df.tail(3))
        
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Error HTTP: {http_err}")
        print(f"Respuesta del servidor: {response.text}")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado: {e}")


# ============================================================
# 🚀 Ejecución principal
# ============================================================
if __name__ == "__main__":
    
    # Coordenadas de Gustavo A. Madero (CDMX)
    LATITUD = 19.5047
    LONGITUD = -99.1469

    # Rango de fechas: desde 2024-01-01 hasta ayer
    FECHA_INICIO = "2024-01-01"
    FECHA_FIN = datetime.now().strftime("%Y-%m-%d")
    
    print(f"📆 Descargando datos desde {FECHA_INICIO} hasta {FECHA_FIN}")
    
    ARCHIVO_SALIDA = "historico_clima_2024-2025_CDMX2.csv"
    
    # Eliminar archivo previo si existe
    if os.path.exists(ARCHIVO_SALIDA):
        os.remove(ARCHIVO_SALIDA)
        print(f"🧹 Archivo anterior '{ARCHIVO_SALIDA}' eliminado.")
    
    descargar_datos_historicos(
        lat=LATITUD,
        lon=LONGITUD,
        start_date=FECHA_INICIO,
        end_date=FECHA_FIN,
        archivo_csv=ARCHIVO_SALIDA
    )
