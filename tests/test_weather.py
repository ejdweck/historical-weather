import pytest
from datetime import datetime, date
import pandas as pd
import responses  # for mocking HTTP requests
from scripts.fetch_weather import fetch_historical_weather, save_weather_data
from app.core.config import settings

# Sample API response data
MOCK_WEATHER_RESPONSE = {
    "daily": {
        "time": ["2024-01-01", "2024-01-02"],
        "temperature_2m_max": [75.0, 80.0],
        "temperature_2m_min": [65.0, 70.0],
        "temperature_2m_mean": [70.0, 75.0]
    }
}

@pytest.fixture
def mock_weather_api():
    """Fixture to mock the Open-Meteo API response"""
    with responses.RequestsMock() as rsps:
        # Mock any URL that starts with the Open-Meteo API base
        rsps.add(
            responses.GET,
            url=responses.matchers.URLMatcher('https://archive-api.open-meteo.com'),
            json=MOCK_WEATHER_RESPONSE,
            status=200
        )
        yield rsps

def test_fetch_weather_success(mock_weather_api):
    """Test successful weather data fetching"""
    df = fetch_historical_weather()
    
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Our mock data has 2 days
    
    # Check calculated fields
    assert df['cdd'].iloc[0] == 5.0  # 70.0 - 65 = 5
    assert df['hdd'].iloc[0] == 0.0  # No HDD when temp > 65
    
    # Check data types
    assert isinstance(df['date'].iloc[0], pd.Timestamp)
    assert isinstance(df['high_temp'].iloc[0], float)

def test_save_weather_data(test_db):
    """Test saving weather data to database"""
    # Create test DataFrame
    test_data = pd.DataFrame({
        'date': [datetime(2024, 1, 1), datetime(2024, 1, 2)],
        'high_temp': [75.0, 80.0],
        'low_temp': [65.0, 70.0],
        'avg_temp': [70.0, 75.0],
        'cdd': [5.0, 10.0],
        'hdd': [0.0, 0.0]
    })
    
    # Save data
    save_weather_data(test_data)
    
    # Query the database to verify
    result = test_db.query(WeatherData).all()
    assert len(result) == 2
    
    # Check first record
    first_record = result[0]
    assert first_record.date == date(2024, 1, 1)
    assert first_record.high_temp == 75.0
    assert first_record.cdd == 5.0
    assert first_record.hdd == 0.0

def test_save_duplicate_dates(test_db):
    """Test handling of duplicate dates when saving"""
    # Create test DataFrame with duplicate dates
    test_data = pd.DataFrame({
        'date': [datetime(2024, 1, 1), datetime(2024, 1, 1)],  # Duplicate date
        'high_temp': [75.0, 80.0],
        'low_temp': [65.0, 70.0],
        'avg_temp': [70.0, 75.0],
        'cdd': [5.0, 10.0],
        'hdd': [0.0, 0.0]
    })
    
    # Save data
    save_weather_data(test_data)
    
    # Verify only one record was saved
    result = test_db.query(WeatherData).all()
    assert len(result) == 1

@responses.activate
def test_fetch_weather_api_error():
    """Test handling of API errors"""
    # Mock API error response
    responses.add(
        responses.GET,
        url=responses.matchers.URLMatcher('https://archive-api.open-meteo.com'),
        status=500
    )
    
    df = fetch_historical_weather()
    assert df is None 