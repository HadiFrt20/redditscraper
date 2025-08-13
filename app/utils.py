# app/utils.py
from typing import List
import pandas as pd

def players_from_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    if "player" not in df.columns:
        raise ValueError("CSV must have a 'player' column")
    names = df["player"].dropna().map(str).str.strip()
    return names[names != ""].unique().tolist()
