"""Central configuration for the Web Vitals Spark Pipeline.

All paths and thresholds are read from environment variables (with sane
defaults) so the pipeline never depends on hardcoded absolute paths.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _data_root() -> Path:
    root = os.environ.get("DATA_ROOT", ".")
    path = Path(root)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


@dataclass(frozen=True)
class Paths:
    root: Path = field(default_factory=_data_root)

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def raw(self) -> Path:
        return self.data / "raw"

    @property
    def raw_lighthouse(self) -> Path:
        return self.raw / "lighthouse"

    @property
    def raw_crux(self) -> Path:
        return self.raw / "crux"

    @property
    def raw_page_load_logs(self) -> Path:
        return self.raw / "page_load_logs"

    @property
    def raw_devices(self) -> Path:
        return self.raw / "devices"

    @property
    def processed(self) -> Path:
        return self.data / "processed"

    @property
    def curated(self) -> Path:
        return self.data / "curated"

    @property
    def reports(self) -> Path:
        return self.root / "reports"


PATHS = Paths()


@dataclass(frozen=True)
class SparkSettings:
    app_name: str = os.environ.get("SPARK_APP_NAME", "web-vitals-spark-pipeline")
    master: str = os.environ.get("SPARK_MASTER", "local[*]")
    shuffle_partitions: str = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
    log_level: str = os.environ.get("SPARK_LOG_LEVEL", "WARN")


SPARK_SETTINGS = SparkSettings()


@dataclass(frozen=True)
class VitalsThresholds:
    """"Good"/threshold cutoffs per the Core Web Vitals spec, used for health
    scoring and for the absolute-value leg of regression detection."""

    lcp_good_ms: int = 2500
    cls_good: float = 0.1
    inp_good_ms: int = 200
    ttfb_good_ms: int = 800


VITALS_THRESHOLDS = VitalsThresholds()


@dataclass(frozen=True)
class RegressionThresholds:
    lcp_regression_pct: float = float(os.environ.get("LCP_REGRESSION_PCT", 0.20))
    lcp_poor_threshold_ms: int = int(os.environ.get("LCP_POOR_THRESHOLD_MS", 2500))

    cls_regression_delta: float = float(os.environ.get("CLS_REGRESSION_DELTA", 0.05))
    cls_poor_threshold: float = float(os.environ.get("CLS_POOR_THRESHOLD", 0.1))

    inp_regression_pct: float = float(os.environ.get("INP_REGRESSION_PCT", 0.20))
    inp_poor_threshold_ms: int = int(os.environ.get("INP_POOR_THRESHOLD_MS", 200))

    ttfb_regression_pct: float = float(os.environ.get("TTFB_REGRESSION_PCT", 0.25))
    ttfb_poor_threshold_ms: int = int(os.environ.get("TTFB_POOR_THRESHOLD_MS", 800))


REGRESSION_THRESHOLDS = RegressionThresholds()

VALID_DEVICE_TYPES = ("mobile", "desktop", "tablet")
