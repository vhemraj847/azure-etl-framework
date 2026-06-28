"""
Azure ETL Framework - Core Pipeline Module
==========================================
A modular, reusable ETL framework for Azure data pipelines.
Supports Bronze → Silver → Gold Medallion Architecture.
"""

import logging
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

# ── Logging Setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Base Extractor ─────────────────────────────────────────────
class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> list[dict]:
        """Extract data from source. Must return list of dicts."""
        pass

    def validate_connection(self) -> bool:
        """Override to add connection validation logic."""
        return True


# ── Base Transformer ───────────────────────────────────────────
class BaseTransformer(ABC):
    """Abstract base class for all data transformers."""

    def __init__(self, layer: str = "silver"):
        """
        Args:
            layer: Target Medallion layer — 'bronze', 'silver', or 'gold'
        """
        self.layer = layer
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, data: list[dict]) -> list[dict]:
        """Apply transformation logic and return cleaned data."""
        pass

    def add_audit_columns(self, records: list[dict]) -> list[dict]:
        """Inject standard audit columns into every record."""
        timestamp = datetime.utcnow().isoformat()
        for record in records:
            record["_ingested_at"] = timestamp
            record["_layer"] = self.layer
            record["_source_system"] = "etl_framework"
        return records


# ── Base Loader ────────────────────────────────────────────────
class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    def __init__(self, target_name: str):
        self.target_name = target_name
        self.logger = logging.getLogger(self.__class__.__name__)
        self._records_loaded = 0

    @abstractmethod
    def load(self, data: list[dict]) -> int:
        """Load data to target. Returns count of records loaded."""
        pass

    @property
    def records_loaded(self) -> int:
        return self._records_loaded


# ── Pipeline Orchestrator ──────────────────────────────────────
class Pipeline:
    """
    Orchestrates Extract → Transform → Load execution.

    Usage:
        pipeline = Pipeline(
            name="sales_bronze_to_silver",
            extractor=SalesExtractor(),
            transformer=SalesTransformer(),
            loader=ADLSLoader(container="silver"),
        )
        result = pipeline.run()
    """

    def __init__(
        self,
        name: str,
        extractor: BaseExtractor,
        transformer: BaseTransformer,
        loader: BaseLoader,
        fail_on_empty: bool = True,
    ):
        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.fail_on_empty = fail_on_empty
        self.logger = logging.getLogger(f"Pipeline.{name}")

    def run(self) -> dict:
        """
        Execute the full ETL pipeline.

        Returns:
            dict with keys: success, records_extracted,
                            records_loaded, duration_seconds, errors
        """
        start = datetime.utcnow()
        result = {
            "pipeline": self.name,
            "success": False,
            "records_extracted": 0,
            "records_loaded": 0,
            "duration_seconds": 0,
            "errors": [],
        }

        try:
            # ── Extract ──
            self.logger.info(f"[{self.name}] Starting extraction from {self.extractor.source_name}")
            raw_data = self.extractor.extract()
            result["records_extracted"] = len(raw_data)
            self.logger.info(f"[{self.name}] Extracted {len(raw_data)} records")

            if not raw_data and self.fail_on_empty:
                raise ValueError("Extractor returned 0 records — pipeline aborted.")

            # ── Transform ──
            self.logger.info(f"[{self.name}] Transforming data → {self.transformer.layer} layer")
            transformed = self.transformer.transform(raw_data)
            self.logger.info(f"[{self.name}] Transformation complete: {len(transformed)} records")

            # ── Load ──
            self.logger.info(f"[{self.name}] Loading to {self.loader.target_name}")
            count = self.loader.load(transformed)
            result["records_loaded"] = count
            self.logger.info(f"[{self.name}] Loaded {count} records successfully")

            result["success"] = True

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            self.logger.error(f"[{self.name}] Pipeline failed — {error_msg}")
            result["errors"].append(error_msg)

        finally:
            duration = (datetime.utcnow() - start).total_seconds()
            result["duration_seconds"] = round(duration, 2)
            self.logger.info(f"[{self.name}] Finished in {duration:.2f}s | success={result['success']}")

        return result


# ── Config Loader ──────────────────────────────────────────────
class PipelineConfig:
    """Load pipeline configuration from a JSON file."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config: Optional[dict] = None

    def load(self) -> dict:
        if self._config is None:
            with open(self.config_path, "r") as f:
                self._config = json.load(f)
            logger.info(f"Config loaded from {self.config_path}")
        return self._config

    def get(self, key: str, default=None):
        return self.load().get(key, default)
