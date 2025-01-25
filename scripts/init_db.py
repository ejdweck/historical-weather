import os
import sys
from pathlib import Path

from utils import add_project_root_to_path
add_project_root_to_path()

from app.db.models import Base
from app.db.database import engine
from app.core.config import settings

def init_db():
    print(f"Connecting to database...")
    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully!")
    except Exception as e:
        print(f"Error creating database tables: {e}")

if __name__ == "__main__":
    init_db() 