"""
Unit Tests — ETL Framework
===========================
Run with:  pytest tests/ -v
"""

import pytest
from etl.pipeline import Pipeline, BaseExtractor, BaseTransformer, BaseLoader
from etl.transformers import MedallionTransformer, SalesTransformer
from etl.loaders import ConsoleLoader


# ── Fixtures ───────────────────────────────────────────────────
class DummyExtractor(BaseExtractor):
    def __init__(self, records):
        super().__init__(source_name="dummy")
        self._records = records

    def extract(self):
        return self._records


class DummyTransformer(BaseTransformer):
    def transform(self, data):
        return self.add_audit_columns(data)


class DummyLoader(BaseLoader):
    def __init__(self):
        super().__init__(target_name="dummy")
        self.loaded = []

    def load(self, data):
        self.loaded = data
        self._records_loaded = len(data)
        return self._records_loaded


SAMPLE_RECORDS = [
    {"OrderID": "1001", "CustID": "C001", "Amt": "500.00", "Region": "UK"},
    {"OrderID": "1002", "CustID": "C002", "Amt": "1500.00", "Region": "UAE"},
    {"OrderID": "1003", "CustID": "C003", "Amt": "12000.00", "Region": "India"},
]


# ── Pipeline Tests ─────────────────────────────────────────────
class TestPipeline:
    def test_successful_run(self):
        loader = DummyLoader()
        pipeline = Pipeline(
            name="test_pipeline",
            extractor=DummyExtractor(SAMPLE_RECORDS),
            transformer=DummyTransformer(),
            loader=loader,
        )
        result = pipeline.run()
        assert result["success"] is True
        assert result["records_extracted"] == 3
        assert result["records_loaded"] == 3
        assert result["errors"] == []

    def test_empty_source_fails(self):
        pipeline = Pipeline(
            name="empty_test",
            extractor=DummyExtractor([]),
            transformer=DummyTransformer(),
            loader=DummyLoader(),
            fail_on_empty=True,
        )
        result = pipeline.run()
        assert result["success"] is False
        assert len(result["errors"]) > 0

    def test_empty_source_allowed(self):
        pipeline = Pipeline(
            name="empty_allowed",
            extractor=DummyExtractor([]),
            transformer=DummyTransformer(),
            loader=DummyLoader(),
            fail_on_empty=False,
        )
        result = pipeline.run()
        assert result["success"] is True


# ── Transformer Tests ──────────────────────────────────────────
class TestMedallionTransformer:
    def test_deduplication(self):
        data = [
            {"id": "1", "value": "a"},
            {"id": "1", "value": "a"},  # duplicate
            {"id": "2", "value": "b"},
        ]
        transformer = MedallionTransformer(dedupe_keys=["id"])
        result = transformer.transform(data)
        assert len(result) == 2

    def test_drop_nulls(self):
        data = [
            {"id": "1", "name": "Alice"},
            {"id": None, "name": "Bob"},   # should be dropped
            {"id": "3", "name": ""},       # empty string — also dropped
        ]
        transformer = MedallionTransformer(drop_nulls_on=["id"])
        result = transformer.transform(data)
        ids = [r["id"] for r in result]
        assert None not in ids
        assert "" not in ids

    def test_rename_columns(self):
        data = [{"OldName": "value"}]
        transformer = MedallionTransformer(rename_map={"OldName": "new_name"})
        result = transformer.transform(data)
        assert "new_name" in result[0]
        assert "OldName" not in result[0]

    def test_audit_columns_added(self):
        transformer = MedallionTransformer(layer="silver")
        result = transformer.transform([{"id": "1"}])
        assert "_ingested_at" in result[0]
        assert result[0]["_layer"] == "silver"


# ── SalesTransformer Tests ─────────────────────────────────────
class TestSalesTransformer:
    def test_revenue_band_high(self):
        data = [{"OrderID": "1", "CustID": "C1", "Amt": "15000"}]
        transformer = SalesTransformer()
        result = transformer.transform(data)
        assert result[0]["revenue_band"] == "high"

    def test_revenue_band_medium(self):
        data = [{"OrderID": "2", "CustID": "C2", "Amt": "2000"}]
        transformer = SalesTransformer()
        result = transformer.transform(data)
        assert result[0]["revenue_band"] == "medium"

    def test_revenue_band_low(self):
        data = [{"OrderID": "3", "CustID": "C3", "Amt": "50"}]
        transformer = SalesTransformer()
        result = transformer.transform(data)
        assert result[0]["revenue_band"] == "low"

    def test_amount_cast_to_float(self):
        data = [{"OrderID": "4", "CustID": "C4", "Amt": "999.99"}]
        transformer = SalesTransformer()
        result = transformer.transform(data)
        assert isinstance(result[0]["amount"], float)


# ── ConsoleLoader Tests ────────────────────────────────────────
class TestConsoleLoader:
    def test_load_returns_count(self, capsys):
        loader = ConsoleLoader(max_preview=2)
        count = loader.load([{"id": 1}, {"id": 2}, {"id": 3}])
        assert count == 3
        assert loader.records_loaded == 3
