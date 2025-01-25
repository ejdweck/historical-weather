from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Date, Float, String, Integer

class Base(DeclarativeBase):
    pass

class WeatherData(Base):
    __tablename__ = "weather_data"
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, nullable=False, index=True)
    high_temp = Column(Float, nullable=False)
    low_temp = Column(Float, nullable=False)
    avg_temp = Column(Float, nullable=False)
    cdd = Column(Float, nullable=False)
    hdd = Column(Float, nullable=False)

class SettlementPrice(Base):
    __tablename__ = "settlement_prices"
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, nullable=False, index=True)
    pipeline = Column(String, nullable=False)
    settlement_price = Column(Float, nullable=False) 