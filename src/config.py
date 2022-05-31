import os
from pathlib import Path


FRED_KEY = os.getenv("FRED_KEY", "")
DATA_DIR = Path(__file__).parent.resolve () / "data"
