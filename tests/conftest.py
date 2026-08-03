"""pytest 共用設定 — 把 repo root 加進 sys.path (tests/ 由 repo root 跑)。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
