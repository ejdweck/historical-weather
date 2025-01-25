import sys
from utils import add_project_root_to_path
add_project_root_to_path()

from app.db.database import engine
from sqlalchemy import text

def verify_tables():
    with engine.connect() as connection:
        # Get all table names
        result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        tables = [row[0] for row in result]
        print("Found tables:", tables)

if __name__ == "__main__":
    verify_tables() 