from datetime import date
from app.db.models import WeatherData, SettlementPrice

def test_weather_data_model(test_db):
    # Create test weather data
    weather = WeatherData(
        date=date(2024, 1, 1),
        high_temp=75.0,
        low_temp=65.0,
        avg_temp=70.0,
        cdd=5.0,
        hdd=0.0
    )
    
    test_db.add(weather)
    test_db.commit()
    
    # Query the data
    result = test_db.query(WeatherData).first()
    assert result.date == date(2024, 1, 1)
    assert result.avg_temp == 70.0

def test_settlement_price_model(test_db):
    # Create test settlement price
    price = SettlementPrice(
        date=date(2024, 1, 1),
        pipeline="Transco Zone 6",
        settlement_price=4.50
    )
    
    test_db.add(price)
    test_db.commit()
    
    # Query the data
    result = test_db.query(SettlementPrice).first()
    assert result.date == date(2024, 1, 1)
    assert result.settlement_price == 4.50 