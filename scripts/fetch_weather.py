import sys
from utils import add_project_root_to_path
add_project_root_to_path()

import requests
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.exc import IntegrityError
from app.db.database import SessionLocal
from app.db.models import WeatherData
from app.core.config import settings

# Reference temperature for CDD/HDD (18.33°C ≈ 65°F)
REFERENCE_TEMP_C = 18.33

def fetch_historical_weather():
    """
    Fetch 5 years of historical weather data from Open-Meteo API.
    All temperatures are in Celsius.
    """
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)  # 5 years

    # Format dates for API
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Open-Meteo API URL
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={settings.LATITUDE}&longitude={settings.LONGITUDE}"
        f"&start_date={start_str}&end_date={end_str}"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean"
        f"&timezone={settings.TIMEZONE}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame (temperatures in Celsius)
        df = pd.DataFrame({
            'date': pd.to_datetime(data['daily']['time']),
            'high_temp': data['daily']['temperature_2m_max'],
            'low_temp': data['daily']['temperature_2m_min'],
            'avg_temp': data['daily']['temperature_2m_mean']
        })

        # Calculate CDD and HDD in Celsius
        df['cdd'] = df['avg_temp'].apply(lambda x: max(0, x - REFERENCE_TEMP_C))
        df['hdd'] = df['avg_temp'].apply(lambda x: max(0, REFERENCE_TEMP_C - x))

        return df

    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

def save_weather_data(df):
    """
    Save weather data to database
    """
    db = SessionLocal()
    try:
        for _, row in df.iterrows():
            weather_data = WeatherData(
                date=row['date'].date(),
                high_temp=row['high_temp'],
                low_temp=row['low_temp'],
                avg_temp=row['avg_temp'],
                cdd=row['cdd'],
                hdd=row['hdd']
            )
            try:
                db.add(weather_data)
                db.commit()
            except IntegrityError:
                # Skip if date already exists
                db.rollback()
                print(f"Data for {row['date'].date()} already exists, skipping...")
                continue
        print("Weather data saved successfully!")
    except Exception as e:
        print(f"Error saving weather data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    print("Fetching historical weather data...")
    df = fetch_historical_weather()
    if df is not None:
        print("Saving data to database...")
        save_weather_data(df) 