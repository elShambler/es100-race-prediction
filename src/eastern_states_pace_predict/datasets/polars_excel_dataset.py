from pathlib import Path
from typing import Any

import polars as pl
from kedro.io import AbstractDataset


class PolarsExcelDataset(AbstractDataset):
    """Kedro dataset that loads an Excel file into a Polars DataFrame via fastexcel."""

    def __init__(self, filepath: str, load_args: dict[str, Any] | None = None):
        self._filepath = Path(filepath)
        self._load_args = load_args or {}

    def _load(self) -> pl.DataFrame:
        return pl.read_excel(self._filepath, **self._load_args)

    def _save(self, data: pl.DataFrame) -> None:
        raise NotImplementedError("PolarsExcelDataset is read-only.")

    def _describe(self) -> dict[str, Any]:
        return {"filepath": str(self._filepath), "load_args": self._load_args}
