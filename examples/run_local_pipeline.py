"""
Example: Run a local pipeline with ConsoleLoader (no Azure needed)
==================================================================
Run with:  python examples/run_local_pipeline.py
"""

from etl.pipeline import Pipeline, BaseExtractor
from etl.transformers import SalesTransformer
from etl.loaders import ConsoleLoader


# ── Mock Extractor (simulates pulling from a source) ──────────
class MockSalesExtractor(BaseExtractor):
    def __init__(self):
        super().__init__(source_name="mock_sales_api")

    def extract(self) -> list[dict]:
        return [
            {"OrderID": "1001", "CustID": "C001", "Amt": "15000.00", "Region": "UK  "},
            {"OrderID": "1002", "CustID": "C002", "Amt": "850.50",   "Region": "UAE "},
            {"OrderID": "1001", "CustID": "C001", "Amt": "15000.00", "Region": "UK  "},  # duplicate
            {"OrderID": "1003", "CustID": None,   "Amt": "200.00",   "Region": "India"}, # null key
            {"OrderID": "1004", "CustID": "C004", "Amt": "5500.00",  "Region": "India"},
        ]


# ── Run Pipeline ───────────────────────────────────────────────
if __name__ == "__main__":
    pipeline = Pipeline(
        name="sales_bronze_to_silver",
        extractor=MockSalesExtractor(),
        transformer=SalesTransformer(),
        loader=ConsoleLoader(max_preview=10),
    )

    result = pipeline.run()

    print("\nPipeline Result:")
    for key, value in result.items():
        print(f"  {key}: {value}")
