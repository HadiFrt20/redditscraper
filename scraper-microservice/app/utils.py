# app/utils.py
from typing import List
import pandas as pd
import re


def players_from_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    if "player" not in df.columns:
        raise ValueError("CSV must have a 'player' column")
    names = df["player"].dropna().map(str).str.strip()
    return names[names != ""].unique().tolist()


def slugify(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", name.strip()).strip("-").lower()
    return slug or "player"
