"""
Data Transformers
==================
Reusable transformation classes for Bronze → Silver → Gold layers.
"""

import logging
from typing import Optional

from etl.pipeline import BaseTransformer

logger = logging.getLogger(__name__)


class MedallionTransformer(BaseTransformer):
    """
    Standard Medallion Architecture transformer.

    Bronze → Silver: deduplication, null handling, type casting
    Silver → Gold:   business aggregations, derived columns

    Args:
        layer:          Target layer ('silver' or 'gold')
        dedupe_keys:    Column names to use for deduplication
        drop_nulls_on: Columns where null records should be dropped
        rename_map:     Dict of {old_name: new_name} column renames
    """

    def __init__(
        self,
        layer: str = "silver",
        dedupe_keys: Optional[list[str]] = None,
        drop_nulls_on: Optional[list[str]] = None,
        rename_map: Optional[dict[str, str]] = None,
    ):
        super().__init__(layer=layer)
        self.dedupe_keys = dedupe_keys or []
        self.drop_nulls_on = drop_nulls_on or []
        self.rename_map = rename_map or {}

    def transform(self, data: list[dict]) -> list[dict]:
        self.logger.info(f"Transforming {len(data)} records to {self.layer} layer")

        result = data.copy()
        result = self._rename_columns(result)
        result = self._drop_nulls(result)
        result = self._deduplicate(result)
        result = self._standardise_strings(result)
        result = self.add_audit_columns(result)

        self.logger.info(f"Transformation complete: {len(result)} records output")
        return result

    def _rename_columns(self, data: list[dict]) -> list[dict]:
        if not self.rename_map:
            return data
        renamed = []
        for record in data:
            new_record = {self.rename_map.get(k, k): v for k, v in record.items()}
            renamed.append(new_record)
        self.logger.debug(f"Renamed columns: {self.rename_map}")
        return renamed

    def _drop_nulls(self, data: list[dict]) -> list[dict]:
        if not self.drop_nulls_on:
            return data
        before = len(data)
        result = [
            r for r in data
            if all(r.get(col) not in (None, "", "NULL") for col in self.drop_nulls_on)
        ]
        dropped = before - len(result)
        if dropped:
            self.logger.warning(f"Dropped {dropped} records with nulls in {self.drop_nulls_on}")
        return result

    def _deduplicate(self, data: list[dict]) -> list[dict]:
        if not self.dedupe_keys:
            return data
        before = len(data)
        seen = set()
        result = []
        for record in data:
            key = tuple(record.get(k) for k in self.dedupe_keys)
            if key not in seen:
                seen.add(key)
                result.append(record)
        dupes = before - len(result)
        if dupes:
            self.logger.info(f"Removed {dupes} duplicate records on keys {self.dedupe_keys}")
        return result

    def _standardise_strings(self, data: list[dict]) -> list[dict]:
        """Strip whitespace and normalise string fields."""
        for record in data:
            for key, value in record.items():
                if isinstance(value, str):
                    record[key] = value.strip()
        return data


class SalesTransformer(MedallionTransformer):
    """
    Domain-specific transformer for sales data.
    Extends MedallionTransformer with sales business rules.
    """

    def __init__(self):
        super().__init__(
            layer="silver",
            dedupe_keys=["order_id"],
            drop_nulls_on=["order_id", "customer_id", "amount"],
            rename_map={"OrderID": "order_id", "CustID": "customer_id", "Amt": "amount"},
        )

    def transform(self, data: list[dict]) -> list[dict]:
        result = super().transform(data)
        result = self._cast_amounts(result)
        result = self._add_revenue_band(result)
        return result

    def _cast_amounts(self, data: list[dict]) -> list[dict]:
        for record in data:
            try:
                record["amount"] = float(record.get("amount", 0))
            except (ValueError, TypeError):
                record["amount"] = 0.0
        return data

    def _add_revenue_band(self, data: list[dict]) -> list[dict]:
        """Derive revenue band — a Gold-layer business rule."""
        for record in data:
            amount = record.get("amount", 0)
            if amount >= 10000:
                record["revenue_band"] = "high"
            elif amount >= 1000:
                record["revenue_band"] = "medium"
            else:
                record["revenue_band"] = "low"
        return data
