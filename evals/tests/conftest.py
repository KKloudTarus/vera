from __future__ import annotations

import sys
from pathlib import Path

DESKTOP = Path(__file__).resolve().parents[2]
if str(DESKTOP) not in sys.path:
    sys.path.insert(0, str(DESKTOP))
