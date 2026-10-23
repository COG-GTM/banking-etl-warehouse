"""Ticket 9 parity harness: legacy DWH fixtures -> Silver, and comparison with the original procs.

``fixtures/t9_legacy_parity/<dataset>/<DimTable>.json`` hold the legacy DWH tables (legacy column
names) and ``expected.json`` the result sets of ``sp_DailyTransaction`` / ``sp_BalancePerCustomer``
run on SQL Server 2022 (see ``fixtures/t9_legacy_parity/build_expected.py``).

Seeding writes Silver tables only, and only into a Ticket 9 scratch namespace (see
``assert_t9_scratch``); it never touches gold or the shared ``bdwh_`` schemas.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from ..common.config import EnvConfig
from ..contracts.tables import DIM_ACCOUNT, DIM_BRANCH, DIM_CUSTOMER, FACT_TRANSACTION, TableContract
from .analytics import FN_BALANCE_PER_CUSTOMER, FN_DAILY_TRANSACTION, gold_object

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "t9_legacy_parity"
DATASETS = ("legacy", "edge")
SEED_TABLES = (DIM_BRANCH, DIM_ACCOUNT, DIM_CUSTOMER, FACT_TRANSACTION)
PROC_TO_FUNCTION = {"sp_DailyTransaction": FN_DAILY_TRANSACTION, "sp_BalancePerCustomer": FN_BALANCE_PER_CUSTOMER}
_T9_PREFIX = re.compile(r"(^|_)t9(_[a-z0-9]+)?_$")


def assert_t9_scratch(cfg: EnvConfig) -> None:
    """Refuse to seed anything but a Ticket 9 scratch namespace (e.g. prefix ``bdwh_t9_``)."""
    if cfg.target == "prod" or not _T9_PREFIX.search(cfg.schema_prefix or ""):
        raise PermissionError(
            f"Refusing to seed fixtures into target={cfg.target!r} schema_prefix={cfg.schema_prefix!r}: "
            "only Ticket 9 scratch prefixes ending in 't9_' (e.g. 'bdwh_t9_') are allowed."
        )


def _convert(value: Any, spark_type: str) -> Any:
    if value is None:
        return None
    t = spark_type.upper()
    if t.startswith("DECIMAL"):
        return Decimal(str(value))
    if t == "TIMESTAMP":
        return datetime.fromisoformat(value)
    if t == "DATE":
        return date.fromisoformat(value[:10])
    if t in ("INT", "BIGINT"):
        return int(value)
    return value


def fixture_rows(contract: TableContract, dataset: str) -> list[tuple]:
    """Legacy DWH rows (legacy column names) mapped onto the Silver contract columns."""
    if dataset not in DATASETS:
        raise ValueError(f"Unknown dataset {dataset!r}; expected one of {DATASETS}")
    legacy = json.loads((FIXTURE_DIR / dataset / f"{contract.legacy_name}.json").read_text(encoding="utf-8"))
    rows = []
    for r in legacy:
        row = []
        for c in contract.columns:
            if c.legacy_name:
                row.append(_convert(r[c.legacy_name], c.type))
            elif c.name == "_source_system":
                row.append(f"t9_fixture_{dataset}")
            else:  # _loaded_at
                row.append(None)
        rows.append(tuple(row))
    return rows


def seed_silver(spark, cfg: EnvConfig, dataset: str, *, guard: bool = True) -> dict[str, int]:
    """Overwrite the four Silver star-schema tables of ``cfg`` with a fixture dataset."""
    if guard:
        assert_t9_scratch(cfg)
    from pyspark.sql import functions as F

    counts = {}
    for contract in SEED_TABLES:
        df = spark.createDataFrame(fixture_rows(contract, dataset), schema=contract.ddl_schema())
        if "_loaded_at" in contract.column_names:
            df = df.withColumn("_loaded_at", F.current_timestamp())
        df.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(cfg.table("silver", contract.name))
        counts[contract.name] = df.count()
    return counts


def expected_cases(dataset: str | None = None) -> list[dict]:
    doc = json.loads((FIXTURE_DIR / "expected.json").read_text(encoding="utf-8"))
    return [c for c in doc["cases"] if dataset is None or c["dataset"] == dataset]


def _norm(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value != 0 else "0"
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return value


def expected_rows(case: dict) -> list[tuple]:
    """Legacy result set renamed to the GoldObject output columns, values normalised."""
    obj = gold_object(PROC_TO_FUNCTION[case["proc"]])
    out = []
    for r in case["rows"]:
        row = []
        for c in obj.output:
            v = r[c.legacy_name]
            row.append(_norm(Decimal(v)) if c.type.upper().startswith("DECIMAL") and v is not None else _norm(v))
        out.append(tuple(row))
    return out


def actual_rows(rows: Iterable[Any], case: dict) -> list[tuple]:
    obj = gold_object(PROC_TO_FUNCTION[case["proc"]])
    return [tuple(_norm(r[c.name]) for c in obj.output) for r in rows]


def compare(actual: list[tuple], case: dict) -> tuple[bool, str]:
    """Daily summary: same rows in the same order (the proc ORDER BYs date). Balance: same multiset
    (the proc has no ORDER BY)."""
    expected = expected_rows(case)
    if case["proc"] == "sp_DailyTransaction":
        ok = actual == expected
    else:
        ok = Counter(actual) == Counter(expected)
    return ok, "" if ok else f"expected {expected} got {actual}"


def case_args(case: dict) -> dict[str, Any]:
    """Named-parameter values for the function call (dates as datetime.date)."""
    obj = gold_object(PROC_TO_FUNCTION[case["proc"]])
    args = {}
    for p in obj.parameters:
        v = case["params"][p.name]
        args[p.name] = date.fromisoformat(v) if (v is not None and p.type == "DATE") else v
    return args
