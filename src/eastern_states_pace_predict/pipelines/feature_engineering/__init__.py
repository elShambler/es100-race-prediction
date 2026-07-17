"""Feature engineering: aid-station stoppage-time imputation and interval
pace features for the 2021-2025 splits."""

from .pipeline import create_pipeline

__all__ = ["create_pipeline"]

__version__ = "0.1"
