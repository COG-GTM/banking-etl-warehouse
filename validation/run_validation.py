#!/usr/bin/env python3
"""
Banking ETL Warehouse - Validation Runner
==========================================

Single entry point to compare legacy SQL Server DWH outputs against the
Databricks target state.  Supports two modes:

  --mode seed   Use static CSV seed files in test_cases/ (default, no DB needed)
  --mode live   Query live legacy + Databricks databases

Usage:
    python run_validation.py --mode seed
    python run_validation.py --mode live --legacy-conn "..." --databricks-host "..." ...

Outputs:
    validation/output/validation_results.json   (structured results)
    validation/output/validation_summary.txt    (human-readable summary)
    validation/output/PARITY_REPORT_FILLED.md   (filled parity report)
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Resolve project paths
# ---------------------------------------------------------------------------
VALIDATION_DIR = Path(__file__).resolve().parent
TEST_CASES_DIR = VALIDATION_DIR / "test_cases"
OUTPUT_DIR = VALIDATION_DIR / "output"

# Add parent so `checks` package is importable when running as a script
sys.path.insert(0, str(VALIDATION_DIR.parent))

from validation.checks.row_counts import (
    check_row_counts_from_dataframes,
    format_row_count_results,
)
from validation.checks.key_aggregates import (
    run_all_aggregate_checks,
    format_aggregate_results,
)
from validation.checks.representative_records import (
    compare_customer_records,
    compare_account_records,
    compare_transaction_records,
    format_record_comparison,
)
from validation.checks.stored_procedure_equivalence import (
    compare_daily_transactions,
    compare_balance_per_customer,
    format_sp_results,
)
from validation.checks.schema_comparison import (
    compare_schemas_from_dataframes,
    validate_against_expected_schema,
    format_schema_results,
)

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

TABLE_NAMES = ["DimCustomer", "DimAccount", "DimBranch", "FactTransaction"]

SEED_FILE_MAP = {
    "DimCustomer": "seed_dim_customer.csv",
    "DimAccount": "seed_dim_account.csv",
    "DimBranch": "seed_dim_branch.csv",
    "FactTransaction": "seed_fact_transaction.csv",
}


def load_seed_tables() -> dict[str, pd.DataFrame]:
    """Load seed CSV files into DataFrames."""
    tables = {}
    for table_name, filename in SEED_FILE_MAP.items():
        filepath = TEST_CASES_DIR / filename
        if not filepath.exists():
            print(f"  WARNING: Seed file not found: {filepath}")
            continue
        df = pd.read_csv(filepath)
        # Normalise column names to match DWH schema
        df = _normalise_columns(df, table_name)
        tables[table_name] = df
        print(f"  Loaded {table_name}: {len(df)} rows from {filename}")
    return tables


def _normalise_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Ensure column names match the DWH schema exactly."""
    rename_map: dict[str, str] = {}
    # CSV columns use snake_case; DWH uses PascalCase
    col_mapping = {
        "transaction_id": "TransactionID",
        "account_id": "AccountID",
        "transaction_date": "TransactionDate",
        "amount": "Amount",
        "transaction_type": "TransactionType",
        "branch_id": "BranchID",
        "customer_id": "CustomerID",
        "customer_name": "CustomerName",
        "address": "Address",
        "city_name": "CityName",
        "state_name": "StateName",
        "age": "Age",
        "gender": "Gender",
        "email": "Email",
        "account_type": "AccountType",
        "balance": "Balance",
        "date_opened": "DateOpened",
        "status": "Status",
        "branch_name": "BranchName",
        "branch_location": "BranchLocation",
    }
    for col in df.columns:
        if col in col_mapping:
            rename_map[col] = col_mapping[col]
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def load_live_legacy_tables(conn_string: str) -> dict[str, pd.DataFrame]:
    """Load tables from a live SQL Server connection."""
    import pyodbc  # noqa: delay import

    conn = pyodbc.connect(conn_string)
    tables = {}
    for table in TABLE_NAMES:
        try:
            tables[table] = pd.read_sql(f"SELECT * FROM {table}", conn)
            print(f"  Loaded {table}: {len(tables[table])} rows from legacy DB")
        except Exception as e:
            print(f"  ERROR loading {table} from legacy: {e}")
    conn.close()
    return tables


def load_live_databricks_tables(
    host: str,
    token: str,
    http_path: str,
    catalog: str,
    schema: str = "gold",
) -> dict[str, pd.DataFrame]:
    """Load tables from a live Databricks connection."""
    from databricks import sql as dbsql  # noqa: delay import

    conn = dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)
    tables = {}
    for table in TABLE_NAMES:
        try:
            full_name = f"{catalog}.{schema}.{table}"
            tables[table] = pd.read_sql(f"SELECT * FROM {full_name}", conn)
            print(f"  Loaded {table}: {len(tables[table])} rows from Databricks")
        except Exception as e:
            print(f"  ERROR loading {table} from Databricks: {e}")
    conn.close()
    return tables


# ---------------------------------------------------------------------------
# Expected-output validators (seed mode only)
# ---------------------------------------------------------------------------

def load_expected_json(filename: str) -> dict:
    """Load an expected output JSON file."""
    filepath = TEST_CASES_DIR / filename
    with open(filepath) as f:
        return json.load(f)


def validate_against_expected_row_counts(tables: dict[str, pd.DataFrame]) -> list[dict]:
    """Validate seed tables against expected_row_counts.json."""
    expected = load_expected_json("expected_row_counts.json")["expected_counts"]
    results = []
    for table, expected_count in expected.items():
        actual = len(tables[table]) if table in tables else None
        results.append({
            "check": f"expected_row_count_{table}",
            "table": table,
            "expected": expected_count,
            "actual": actual,
            "status": "PASS" if actual == expected_count else "FAIL",
        })
    return results


def validate_against_expected_aggregates(tables: dict[str, pd.DataFrame]) -> list[dict]:
    """Validate seed FactTransaction against expected_aggregates.json."""
    expected = load_expected_json("expected_aggregates.json")
    fact = tables.get("FactTransaction")
    if fact is None:
        return [{"check": "expected_aggregates", "status": "ERROR", "message": "FactTransaction not loaded"}]

    results = []

    # Total amount
    actual_total = float(fact["Amount"].sum())
    results.append({
        "check": "expected_total_amount",
        "expected": expected["total_transaction_amount"],
        "actual": actual_total,
        "status": "PASS" if abs(actual_total - expected["total_transaction_amount"]) < 0.01 else "FAIL",
    })

    # Counts by type
    actual_counts = fact["TransactionType"].value_counts().to_dict()
    for txn_type, exp_count in expected["transaction_counts_by_type"].items():
        actual = actual_counts.get(txn_type, 0)
        results.append({
            "check": f"expected_count_{txn_type}",
            "expected": exp_count,
            "actual": actual,
            "status": "PASS" if actual == exp_count else "FAIL",
        })

    # Sums by type
    actual_sums = fact.groupby("TransactionType")["Amount"].sum().to_dict()
    for txn_type, exp_sum in expected["amount_sums_by_type"].items():
        actual = float(actual_sums.get(txn_type, 0))
        results.append({
            "check": f"expected_sum_{txn_type}",
            "expected": exp_sum,
            "actual": actual,
            "status": "PASS" if abs(actual - exp_sum) < 0.01 else "FAIL",
        })

    return results


def validate_against_expected_daily_transactions(
    tables: dict[str, pd.DataFrame],
) -> list[dict]:
    """Validate sp_DailyTransaction logic against expected_daily_transactions.json."""
    from validation.checks.stored_procedure_equivalence import compute_daily_transactions

    expected_data = load_expected_json("expected_daily_transactions.json")
    fact = tables.get("FactTransaction")
    if fact is None:
        return [{"check": "expected_daily_txn", "status": "ERROR"}]

    params = expected_data["parameters"]
    actual = compute_daily_transactions(fact, params["start_date"], params["end_date"])

    results = []
    for exp_row in expected_data["expected_output"]:
        exp_date = pd.to_datetime(exp_row["Date"]).date()
        match_rows = actual[actual["Date"] == exp_date]

        if match_rows.empty:
            results.append({
                "check": f"daily_txn_{exp_row['Date']}",
                "status": "FAIL",
                "message": "Date not found in computed output",
            })
            continue

        actual_row = match_rows.iloc[0]
        count_ok = int(actual_row["TotalTransactions"]) == exp_row["TotalTransactions"]
        amount_ok = abs(float(actual_row["TotalAmount"]) - exp_row["TotalAmount"]) < 0.01

        results.append({
            "check": f"daily_txn_{exp_row['Date']}",
            "expected_count": exp_row["TotalTransactions"],
            "actual_count": int(actual_row["TotalTransactions"]),
            "expected_amount": exp_row["TotalAmount"],
            "actual_amount": float(actual_row["TotalAmount"]),
            "status": "PASS" if (count_ok and amount_ok) else "FAIL",
        })

    return results


def validate_against_expected_balance(tables: dict[str, pd.DataFrame]) -> list[dict]:
    """Validate sp_BalancePerCustomer logic against expected_balance_per_customer.json."""
    from validation.checks.stored_procedure_equivalence import compute_balance_per_customer

    expected_data = load_expected_json("expected_balance_per_customer.json")
    results = []

    for tc in expected_data["test_cases"]:
        customer_name = tc["customer_name"]
        actual = compute_balance_per_customer(
            tables["FactTransaction"],
            tables["DimAccount"],
            tables["DimCustomer"],
            customer_name,
        )

        for exp_row in tc["expected_output"]:
            match = actual[
                (actual["CustomerName"] == exp_row["CustomerName"])
                & (actual["AccountType"] == exp_row["AccountType"])
            ]

            if match.empty:
                results.append({
                    "check": f"balance_{customer_name}_{exp_row['AccountType']}",
                    "status": "FAIL",
                    "message": "Record not found in computed output",
                })
                continue

            actual_row = match.iloc[0]
            init_ok = abs(float(actual_row["InitialBalance"]) - exp_row["InitialBalance"]) < 0.01
            curr_ok = abs(float(actual_row["CurrentBalance"]) - exp_row["CurrentBalance"]) < 0.01

            results.append({
                "check": f"balance_{customer_name}_{exp_row['AccountType']}",
                "expected_initial": exp_row["InitialBalance"],
                "actual_initial": float(actual_row["InitialBalance"]),
                "expected_current": exp_row["CurrentBalance"],
                "actual_current": float(actual_row["CurrentBalance"]),
                "status": "PASS" if (init_ok and curr_ok) else "FAIL",
            })

    return results


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_filled_parity_report(all_results: dict) -> str:
    """Generate a filled parity report from validation results."""
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Compute overall status
    all_statuses = []
    for section_results in all_results.values():
        if isinstance(section_results, list):
            all_statuses.extend(r.get("status", "UNKNOWN") for r in section_results)
        elif isinstance(section_results, dict):
            all_statuses.append(section_results.get("status", "UNKNOWN"))

    pass_count = sum(1 for s in all_statuses if s == "PASS")
    fail_count = sum(1 for s in all_statuses if s == "FAIL")
    total = len(all_statuses)
    overall = "ALL PASS" if fail_count == 0 else f"{fail_count} FAILURES out of {total} checks"

    lines = [
        "# Parity Report: Legacy SQL Server DWH vs Databricks Target",
        "",
        f"> **Generated by:** `validation/run_validation.py`",
        f"> **Date:** {now}",
        f"> **Overall Status:** {overall} ({pass_count} passed, {fail_count} failed, {total} total)",
        "",
        "---",
        "",
    ]

    # Row counts section
    if "row_counts" in all_results:
        lines.append("## Row Counts\n")
        lines.append("| Table | Legacy | Databricks | Diff | Status |")
        lines.append("|-------|--------|------------|------|--------|")
        for r in all_results["row_counts"]:
            lines.append(
                f"| {r['table']} | {r.get('legacy_count', 'N/A')} | "
                f"{r.get('databricks_count', 'N/A')} | {r.get('difference', 'N/A')} | "
                f"{r['status']} |"
            )
        lines.append("")

    # Schema section
    if "schema" in all_results:
        lines.append("## Schema Comparison\n")
        for table_result in all_results["schema"]:
            lines.append(f"### {table_result['table']} - {table_result['status']}\n")
            if table_result.get("column_results"):
                lines.append("| Column | Legacy Type | Databricks Type | Status |")
                lines.append("|--------|------------|-----------------|--------|")
                for c in table_result["column_results"]:
                    l_type = c.get("legacy_type") or c.get("expected_type") or "N/A"
                    d_type = c.get("databricks_type") or c.get("actual_type") or "N/A"
                    lines.append(f"| {c['column']} | {l_type} | {d_type} | {c['status']} |")
                lines.append("")

    # Aggregates section
    if "aggregates" in all_results:
        lines.append("## Key Aggregates\n")
        lines.append("| Check | Legacy | Databricks | Diff | Status |")
        lines.append("|-------|--------|------------|------|--------|")
        for r in all_results["aggregates"]:
            lines.append(
                f"| {r['check']} | {r.get('legacy_value', 'N/A')} | "
                f"{r.get('databricks_value', 'N/A')} | {r.get('difference', 'N/A')} | "
                f"{r['status']} |"
            )
        lines.append("")

    # Representative records section
    if "representative_records" in all_results:
        lines.append("## Representative Records\n")
        for section_name, records in all_results["representative_records"].items():
            lines.append(f"### {section_name}\n")
            for r in records:
                lines.append(f"**{r['key_column']} = {r['key_value']}**: {r['status']}")
                if r.get("field_results"):
                    lines.append("| Column | Legacy | Databricks | Match |")
                    lines.append("|--------|--------|------------|-------|")
                    for f in r["field_results"]:
                        lines.append(
                            f"| {f['column']} | {f['legacy_value']} | "
                            f"{f['databricks_value']} | {'Yes' if f['match'] else 'No'} |"
                        )
                lines.append("")

    # Stored procedure section
    if "sp_daily_transaction" in all_results:
        sp = all_results["sp_daily_transaction"]
        lines.append("## sp_DailyTransaction Equivalence\n")
        lines.append(f"**Parameters:** {sp['parameters']}")
        lines.append(f"**Status:** {sp['status']}\n")
        if sp.get("detail"):
            lines.append("| Date | Legacy Count | DB Count | Legacy Amount | DB Amount | Status |")
            lines.append("|------|-------------|----------|--------------|-----------|--------|")
            for d in sp["detail"]:
                lines.append(
                    f"| {d['date']} | {d.get('legacy_count', 'N/A')} | "
                    f"{d.get('databricks_count', 'N/A')} | "
                    f"{d.get('legacy_amount', 'N/A')} | "
                    f"{d.get('databricks_amount', 'N/A')} | {d['status']} |"
                )
            lines.append("")

    if "sp_balance_per_customer" in all_results:
        sp = all_results["sp_balance_per_customer"]
        lines.append("## sp_BalancePerCustomer Equivalence\n")
        lines.append(f"**Parameters:** {sp['parameters']}")
        lines.append(f"**Status:** {sp['status']}\n")
        if sp.get("detail"):
            lines.append("| Customer | AccountType | Legacy Initial | DB Initial | Legacy Current | DB Current | Status |")
            lines.append("|----------|------------|---------------|-----------|---------------|-----------|--------|")
            for d in sp["detail"]:
                lines.append(
                    f"| {d.get('customer_name', 'N/A')} | {d.get('account_type', 'N/A')} | "
                    f"{d.get('legacy_initial_balance', 'N/A')} | "
                    f"{d.get('databricks_initial_balance', 'N/A')} | "
                    f"{d.get('legacy_current_balance', 'N/A')} | "
                    f"{d.get('databricks_current_balance', 'N/A')} | {d['status']} |"
                )
            lines.append("")

    # Expected output validations (seed mode)
    if "expected_validations" in all_results:
        lines.append("## Canonical Test Case Validations (Seed Data)\n")
        lines.append("| Check | Expected | Actual | Status |")
        lines.append("|-------|----------|--------|--------|")
        for r in all_results["expected_validations"]:
            lines.append(
                f"| {r.get('check', 'N/A')} | {r.get('expected', r.get('expected_count', r.get('expected_initial', 'N/A')))} | "
                f"{r.get('actual', r.get('actual_count', r.get('actual_initial', 'N/A')))} | {r['status']} |"
            )
        lines.append("")

    lines.append("---\n")
    lines.append("See [KNOWN_GAPS.md](KNOWN_GAPS.md) for documented intentional deviations.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_seed_validation() -> dict:
    """Run all validations using seed data (both sides identical)."""
    print("\n=== Loading Seed Data ===")
    tables = load_seed_tables()

    if not tables:
        print("ERROR: No seed tables loaded. Aborting.")
        sys.exit(1)

    all_results: dict = {}

    # --- Row Counts (legacy == databricks since same seed data) ---
    print("\n=== Row Count Checks ===")
    row_results = check_row_counts_from_dataframes(tables, tables)
    all_results["row_counts"] = row_results
    print(format_row_count_results(row_results))

    # --- Schema Comparison ---
    print("\n=== Schema Comparison ===")
    schema_results = compare_schemas_from_dataframes(tables, tables)
    all_results["schema"] = schema_results
    print(format_schema_results(schema_results))

    # Also validate against expected schema
    expected_schema_results = validate_against_expected_schema(tables, "seed")
    all_results["expected_schema"] = expected_schema_results

    # --- Key Aggregates ---
    print("\n=== Key Aggregate Checks ===")
    agg_results = run_all_aggregate_checks(tables, tables)
    all_results["aggregates"] = agg_results
    print(format_aggregate_results(agg_results))

    # --- Representative Records ---
    print("\n=== Representative Record Checks ===")
    rep_results = {}

    customer_ids = [1, 3, 5]
    rep_results["DimCustomer"] = compare_customer_records(tables["DimCustomer"], tables["DimCustomer"], customer_ids)
    print(f"  DimCustomer spot-checks: {format_record_comparison(rep_results['DimCustomer'])}")

    account_ids = [101, 104, 107]
    rep_results["DimAccount"] = compare_account_records(tables["DimAccount"], tables["DimAccount"], account_ids)
    print(f"  DimAccount spot-checks: {format_record_comparison(rep_results['DimAccount'])}")

    txn_ids = [1, 7, 14]
    rep_results["FactTransaction"] = compare_transaction_records(
        tables["FactTransaction"], tables["FactTransaction"], txn_ids
    )
    print(f"  FactTransaction spot-checks: {format_record_comparison(rep_results['FactTransaction'])}")

    all_results["representative_records"] = rep_results

    # --- Stored Procedure Equivalence ---
    print("\n=== Stored Procedure Equivalence ===")

    sp_daily = compare_daily_transactions(
        tables["FactTransaction"],
        tables["FactTransaction"],
        start_date="2024-01-18",
        end_date="2024-01-22",
    )
    all_results["sp_daily_transaction"] = sp_daily
    print(format_sp_results(sp_daily))

    sp_balance = compare_balance_per_customer(tables, tables, customer_name="John")
    all_results["sp_balance_per_customer"] = sp_balance
    print(format_sp_results(sp_balance))

    # --- Expected Output Validations ---
    print("\n=== Canonical Expected Output Validations ===")
    expected_checks: list[dict] = []
    expected_checks.extend(validate_against_expected_row_counts(tables))
    expected_checks.extend(validate_against_expected_aggregates(tables))
    expected_checks.extend(validate_against_expected_daily_transactions(tables))
    expected_checks.extend(validate_against_expected_balance(tables))
    all_results["expected_validations"] = expected_checks

    for r in expected_checks:
        status_marker = "PASS" if r["status"] == "PASS" else "FAIL"
        print(f"  [{status_marker}] {r['check']}")

    return all_results


def run_live_validation(args: argparse.Namespace) -> dict:
    """Run all validations using live database connections."""
    print("\n=== Loading Legacy Data ===")
    legacy_tables = load_live_legacy_tables(args.legacy_conn)

    print("\n=== Loading Databricks Data ===")
    databricks_tables = load_live_databricks_tables(
        host=args.databricks_host,
        token=args.databricks_token,
        http_path=args.databricks_http_path,
        catalog=args.databricks_catalog,
        schema=args.databricks_schema,
    )

    all_results: dict = {}

    # Row Counts
    print("\n=== Row Count Checks ===")
    row_results = check_row_counts_from_dataframes(legacy_tables, databricks_tables)
    all_results["row_counts"] = row_results
    print(format_row_count_results(row_results))

    # Schema
    print("\n=== Schema Comparison ===")
    schema_results = compare_schemas_from_dataframes(legacy_tables, databricks_tables)
    all_results["schema"] = schema_results
    print(format_schema_results(schema_results))

    # Aggregates
    print("\n=== Key Aggregate Checks ===")
    agg_results = run_all_aggregate_checks(legacy_tables, databricks_tables)
    all_results["aggregates"] = agg_results
    print(format_aggregate_results(agg_results))

    # Representative Records
    print("\n=== Representative Record Checks ===")
    rep_results = {}

    if "DimCustomer" in legacy_tables and "DimCustomer" in databricks_tables:
        customer_ids = sorted(legacy_tables["DimCustomer"]["CustomerID"].head(3).tolist())
        rep_results["DimCustomer"] = compare_customer_records(
            legacy_tables["DimCustomer"], databricks_tables["DimCustomer"], customer_ids
        )

    if "DimAccount" in legacy_tables and "DimAccount" in databricks_tables:
        account_ids = sorted(legacy_tables["DimAccount"]["AccountID"].head(3).tolist())
        rep_results["DimAccount"] = compare_account_records(
            legacy_tables["DimAccount"], databricks_tables["DimAccount"], account_ids
        )

    if "FactTransaction" in legacy_tables and "FactTransaction" in databricks_tables:
        txn_ids = sorted(legacy_tables["FactTransaction"]["TransactionID"].head(3).tolist())
        rep_results["FactTransaction"] = compare_transaction_records(
            legacy_tables["FactTransaction"], databricks_tables["FactTransaction"], txn_ids
        )

    all_results["representative_records"] = rep_results

    # Stored Procedures
    print("\n=== Stored Procedure Equivalence ===")

    if "FactTransaction" in legacy_tables and "FactTransaction" in databricks_tables:
        sp_daily = compare_daily_transactions(
            legacy_tables["FactTransaction"],
            databricks_tables["FactTransaction"],
            start_date="2024-01-18",
            end_date="2024-01-22",
        )
        all_results["sp_daily_transaction"] = sp_daily
        print(format_sp_results(sp_daily))

    if all(t in legacy_tables for t in TABLE_NAMES[:3]) and all(t in databricks_tables for t in TABLE_NAMES[:3]):
        sp_balance = compare_balance_per_customer(
            legacy_tables, databricks_tables, customer_name="John"
        )
        all_results["sp_balance_per_customer"] = sp_balance
        print(format_sp_results(sp_balance))

    return all_results


def write_outputs(all_results: dict) -> None:
    """Write validation outputs to files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # JSON results
    json_path = OUTPUT_DIR / "validation_results.json"
    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nJSON results written to: {json_path}")

    # Human-readable summary
    summary_path = OUTPUT_DIR / "validation_summary.txt"
    summary_lines = []
    summary_lines.append("=" * 70)
    summary_lines.append("BANKING ETL WAREHOUSE - VALIDATION SUMMARY")
    summary_lines.append(
        f"Generated: {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    summary_lines.append("=" * 70)

    # Count pass/fail
    all_statuses = []
    for section_results in all_results.values():
        if isinstance(section_results, list):
            all_statuses.extend(r.get("status", "UNKNOWN") for r in section_results)
        elif isinstance(section_results, dict):
            if "status" in section_results:
                all_statuses.append(section_results["status"])
            if "detail" in section_results:
                all_statuses.extend(d.get("status", "UNKNOWN") for d in section_results["detail"])

    pass_count = sum(1 for s in all_statuses if s == "PASS")
    fail_count = sum(1 for s in all_statuses if s == "FAIL")
    other_count = len(all_statuses) - pass_count - fail_count

    summary_lines.append(f"\nTotal checks:  {len(all_statuses)}")
    summary_lines.append(f"  Passed:      {pass_count}")
    summary_lines.append(f"  Failed:      {fail_count}")
    summary_lines.append(f"  Other:       {other_count}")
    summary_lines.append(f"\nOverall:       {'ALL PASS' if fail_count == 0 else 'FAILURES DETECTED'}")

    # Section summaries
    for section, data in all_results.items():
        summary_lines.append(f"\n--- {section} ---")
        if isinstance(data, list):
            section_pass = sum(1 for r in data if r.get("status") == "PASS")
            summary_lines.append(f"  {section_pass}/{len(data)} passed")
        elif isinstance(data, dict):
            if "status" in data:
                summary_lines.append(f"  Status: {data['status']}")
            if isinstance(data, dict) and not isinstance(data.get("status"), str):
                # Nested dict like representative_records
                for sub_key, sub_data in data.items():
                    if isinstance(sub_data, list):
                        sub_pass = sum(1 for r in sub_data if r.get("status") == "PASS")
                        summary_lines.append(f"  {sub_key}: {sub_pass}/{len(sub_data)} passed")

    summary_text = "\n".join(summary_lines)
    with open(summary_path, "w") as f:
        f.write(summary_text)
    print(f"Summary written to: {summary_path}")
    print(summary_text)

    # Filled parity report
    report_path = OUTPUT_DIR / "PARITY_REPORT_FILLED.md"
    report_text = generate_filled_parity_report(all_results)
    with open(report_path, "w") as f:
        f.write(report_text)
    print(f"\nFilled parity report written to: {report_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Banking ETL Warehouse Validation Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with seed data (no database needed):
  python run_validation.py --mode seed

  # Run with live connections:
  python run_validation.py --mode live \\
      --legacy-conn "DRIVER={ODBC Driver 17 for SQL Server};SERVER=...;DATABASE=DWH;UID=...;PWD=..." \\
      --databricks-host "your-workspace.cloud.databricks.com" \\
      --databricks-token "your-token" \\
      --databricks-http-path "/sql/1.0/warehouses/abc123" \\
      --databricks-catalog "your_catalog"
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["seed", "live"],
        default="seed",
        help="Validation mode: 'seed' uses static CSV files, 'live' queries databases (default: seed)",
    )
    parser.add_argument("--legacy-conn", help="ODBC connection string for legacy SQL Server DWH")
    parser.add_argument("--databricks-host", help="Databricks workspace hostname")
    parser.add_argument("--databricks-token", help="Databricks access token")
    parser.add_argument("--databricks-http-path", help="Databricks SQL warehouse HTTP path")
    parser.add_argument("--databricks-catalog", help="Databricks Unity Catalog name")
    parser.add_argument("--databricks-schema", default="gold", help="Databricks schema (default: gold)")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 70)
    print("BANKING ETL WAREHOUSE - VALIDATION RUNNER")
    print(f"Mode: {args.mode}")
    print("=" * 70)

    if args.mode == "seed":
        all_results = run_seed_validation()
    else:
        if not args.legacy_conn:
            print("ERROR: --legacy-conn is required in live mode")
            sys.exit(1)
        if not args.databricks_host or not args.databricks_token:
            print("ERROR: --databricks-host and --databricks-token are required in live mode")
            sys.exit(1)
        all_results = run_live_validation(args)

    write_outputs(all_results)

    # Exit with code 1 if any failures
    all_statuses = []
    for section_results in all_results.values():
        if isinstance(section_results, list):
            all_statuses.extend(r.get("status", "UNKNOWN") for r in section_results)
        elif isinstance(section_results, dict):
            all_statuses.append(section_results.get("status", "UNKNOWN"))

    if any(s == "FAIL" for s in all_statuses):
        print("\nValidation completed with FAILURES.")
        sys.exit(1)
    else:
        print("\nAll validations PASSED.")
        sys.exit(0)


if __name__ == "__main__":
    main()
