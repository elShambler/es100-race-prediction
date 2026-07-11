from kedro_datasets._typing import TablePreview
from kedro_datasets.polars import CSVDataset


class PolarsPreviewCSVDataset(CSVDataset):
    """polars.CSVDataset with a preview() method for Kedro Viz table display."""

    def preview(self, nrows: int = 50) -> TablePreview:
        data = self.load().head(nrows)
        return {
            "columns": data.columns,
            "index": list(range(len(data))),
            "data": [list(row) for row in data.rows()],
        }
