"""
Utility: Import functions from PRESCIENT pipeline
Sets up path to access b1.py, b2.py, b3.py from ../PRESCIENT/backend/pipeline/
"""

import sys
from pathlib import Path
import importlib.util

# Add PRESCIENT backend to Python path
_current_file = Path(__file__).resolve()
_pm4py_root = _current_file.parent.parent  # PM4PY/
_prescient_backend = _pm4py_root / "PRESCIENT" / "backend"

# Add PRESCIENT backend to sys.path so imports work properly
if str(_prescient_backend) not in sys.path:
    sys.path.append(str(_prescient_backend))

# Load PRESCIENT b1 module directly by file path (avoids namespace conflict)
spec_b1 = importlib.util.spec_from_file_location(
    "prescient_b1", 
    _prescient_backend / "pipeline" / "b1.py"
)
prescient_b1 = importlib.util.module_from_spec(spec_b1)

# CRITICAL: Register module in sys.modules BEFORE executing it
# This is required for dataclasses to work properly
sys.modules['prescient_b1'] = prescient_b1
spec_b1.loader.exec_module(prescient_b1)

# Export the normalize function for use in main.py
_normalize_value = prescient_b1._normalize_value


if __name__ == "__main__":
    print("✓ Successfully imported from PRESCIENT pipeline")
    print(f"  PRESCIENT backend path: {_prescient_backend}")
    print(f"  _normalize_value function loaded: {_normalize_value}")
