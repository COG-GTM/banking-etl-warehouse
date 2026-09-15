"""Configuration for the gold layer (fact table, analytics, orchestration).

Table names are configurable so the gold jobs can run against the bronze/silver
tables produced by the other migration slices without hard-coding them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

# Talend `tUnite` merge order in Load_FactTransaction (mergeOrder attribute on the
# FLOW connections): 1 = tMSSqlInput (sample.dbo.transaction_db), 2 = tFileInputExcel,
# 3 = tFileInputDelimited. The order decides which row survives deduplication.
DEFAULT_SOURCE_PRIORITY: tuple[str, ...] = ("mssql", "excel", "csv")

DEFAULT_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"


@dataclass(frozen=True)
class TransactionSource:
    """One bronze input feeding the fact table."""

    name: str
    table: str
    priority: int


@dataclass(frozen=True)
class GoldConfig:
    catalog: str = "banking"
    bronze_schema: str = "bronze"
    gold_schema: str = "gold"
    fact_table: str = "fact_transaction"
    orphan_table: str = "fact_transaction_orphan"
    duplicate_table: str = "fact_transaction_duplicate"
    dim_account_table: str = "dim_account"
    dim_branch_table: str = "dim_branch"
    dim_customer_table: str = "dim_customer"
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT
    source_tables: Mapping[str, str] = field(
        default_factory=lambda: {
            "mssql": "mssql_transaction",
            "excel": "excel_transaction",
            "csv": "csv_transaction",
        }
    )
    source_priority: Sequence[str] = DEFAULT_SOURCE_PRIORITY

    def fq(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    @property
    def fact_fqn(self) -> str:
        return self.fq(self.gold_schema, self.fact_table)

    @property
    def orphan_fqn(self) -> str:
        return self.fq(self.gold_schema, self.orphan_table)

    @property
    def duplicate_fqn(self) -> str:
        return self.fq(self.gold_schema, self.duplicate_table)

    @property
    def dim_account_fqn(self) -> str:
        return self.fq(self.gold_schema, self.dim_account_table)

    @property
    def dim_branch_fqn(self) -> str:
        return self.fq(self.gold_schema, self.dim_branch_table)

    @property
    def dim_customer_fqn(self) -> str:
        return self.fq(self.gold_schema, self.dim_customer_table)

    def sources(self) -> list[TransactionSource]:
        """Bronze transaction sources ordered by legacy tUnite merge order."""
        ordered: list[TransactionSource] = []
        for priority, name in enumerate(self.source_priority, start=1):
            table = self.source_tables.get(name)
            if table is None:
                raise KeyError(f"no bronze table configured for source {name!r}")
            ordered.append(
                TransactionSource(
                    name=name,
                    table=self.fq(self.bronze_schema, table),
                    priority=priority,
                )
            )
        return ordered

    def priority_of(self, source_name: str) -> int:
        try:
            return list(self.source_priority).index(source_name) + 1
        except ValueError as exc:  # pragma: no cover - defensive
            raise KeyError(f"unknown source {source_name!r}") from exc
