import sys
from utils import add_project_root_to_path
add_project_root_to_path()

from sqlalchemy import text
from app.db.database import engine
from app.db.models import Base, WeatherData, SettlementPrice

def clean_database():
    """
    Clean all data from the database tables.
    Use with caution - this will delete all data!
    """
    try:
        print("Connecting to database...")
        with engine.connect() as connection:
            # Start a transaction
            with connection.begin():
                print("Deleting all weather data...")
                connection.execute(text("TRUNCATE TABLE weather_data CASCADE"))
                print("Deleting all settlement prices...")
                connection.execute(text("TRUNCATE TABLE settlement_prices CASCADE"))
        print("Database cleaned successfully!")
    except Exception as e:
        print(f"Error cleaning database: {e}")

def drop_and_recreate_tables():
    """
    Drop and recreate all tables.
    Use with caution - this will delete all data and schema!
    """
    try:
        print("Dropping all tables...")
        Base.metadata.drop_all(bind=engine)
        print("Recreating tables...")
        Base.metadata.create_all(bind=engine)
        print("Tables recreated successfully!")
    except Exception as e:
        print(f"Error recreating tables: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Clean database data')
    parser.add_argument('--recreate', action='store_true', 
                      help='Drop and recreate all tables (more destructive)')
    args = parser.parse_args()

    if args.recreate:
        response = input("This will drop and recreate all tables. Are you sure? (y/N): ")
        if response.lower() == 'y':
            drop_and_recreate_tables()
    else:
        response = input("This will delete all data from existing tables. Are you sure? (y/N): ")
        if response.lower() == 'y':
            clean_database() 