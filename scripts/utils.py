import os
import sys
from pathlib import Path

def add_project_root_to_path():
    """Add the project root directory to Python path."""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root)) 