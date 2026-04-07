"""
Representative Record Validation
Compares specific customer/account/transaction records between legacy and Databricks
to ensure exact field-level parity.
"""

import pandas as pd


def compare_records_by_key(
    legacy_df: pd.DataFrame,
    databricks_df: pd.DataFrame,
    key_col: str,
    key_values: list,
    columns_to_compare: list[str] | None = None,
    tolerance: float = 0.01,
) -> list[dict]:
    """Compare specific records by primary key between two DataFrames.

    Args:
        legacy_df: DataFrame from legacy system.
        databricks_df: DataFrame from Databricks.
        key_col: Primary key column name.
        key_values: List of key values to spot-check.
        columns_to_compare: Columns to compare. If None, uses all shared columns.
        tolerance: Numeric tolerance for floating-point comparisons.

    Returns:
        List of result dicts with field-level comparison details.
    """
    results = []

    if columns_to_compare is None:
        columns_to_compare = sorted(
            set(legacy_df.columns) & set(databricks_df.columns) - {key_col}
        )

    for key_val in key_values:
        legacy_row = legacy_df[legacy_df[key_col] == key_val]
        db_row = databricks_df[databricks_df[key_col] == key_val]

        if legacy_row.empty and db_row.empty:
            results.append({
                "key_column": key_col,
                "key_value": key_val,
                "status": "MISSING_IN_BOTH",
                "field_results": [],
            })
            continue
        elif legacy_row.empty:
            results.append({
                "key_column": key_col,
                "key_value": key_val,
                "status": "MISSING_IN_LEGACY",
                "field_results": [],
            })
            continue
        elif db_row.empty:
            results.append({
                "key_column": key_col,
                "key_value": key_val,
                "status": "MISSING_IN_DATABRICKS",
                "field_results": [],
            })
            continue

        legacy_record = legacy_row.iloc[0]
        db_record = db_row.iloc[0]
        field_results = []
        all_match = True

        for col in columns_to_compare:
            legacy_val = legacy_record.get(col)
            db_val = db_record.get(col)

            if legacy_val is None and db_val is None:
                match = True
            elif pd.isna(legacy_val) and pd.isna(db_val):
                match = True
            elif isinstance(legacy_val, (int, float)) and isinstance(db_val, (int, float)):
                match = abs(float(legacy_val) - float(db_val)) <= tolerance
            else:
                match = str(legacy_val) == str(db_val)

            if not match:
                all_match = False

            field_results.append({
                "column": col,
                "legacy_value": _serialize_value(legacy_val),
                "databricks_value": _serialize_value(db_val),
                "match": match,
            })

        results.append({
            "key_column": key_col,
            "key_value": key_val,
            "status": "PASS" if all_match else "FAIL",
            "field_results": field_results,
        })

    return results


def _serialize_value(val):
    """Convert a value to a JSON-serializable form."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def compare_customer_records(
    legacy_customers: pd.DataFrame,
    databricks_customers: pd.DataFrame,
    customer_ids: list[int],
) -> list[dict]:
    """Compare specific DimCustomer records."""
    return compare_records_by_key(
        legacy_customers,
        databricks_customers,
        key_col="CustomerID",
        key_values=customer_ids,
    )


def compare_account_records(
    legacy_accounts: pd.DataFrame,
    databricks_accounts: pd.DataFrame,
    account_ids: list[int],
) -> list[dict]:
    """Compare specific DimAccount records."""
    return compare_records_by_key(
        legacy_accounts,
        databricks_accounts,
        key_col="AccountID",
        key_values=account_ids,
    )


def compare_transaction_records(
    legacy_transactions: pd.DataFrame,
    databricks_transactions: pd.DataFrame,
    transaction_ids: list[int],
) -> list[dict]:
    """Compare specific FactTransaction records."""
    return compare_records_by_key(
        legacy_transactions,
        databricks_transactions,
        key_col="TransactionID",
        key_values=transaction_ids,
    )


def format_record_comparison(results: list[dict]) -> str:
    """Format record comparison results as a human-readable report."""
    lines = []

    for r in results:
        lines.append(f"\n  {r['key_column']} = {r['key_value']}: {r['status']}")

        if r["field_results"]:
            for f in r["field_results"]:
                indicator = "OK" if f["match"] else "MISMATCH"
                lines.append(
                    f"    {f['column']:<25} "
                    f"legacy={f['legacy_value']!s:<20} "
                    f"databricks={f['databricks_value']!s:<20} "
                    f"[{indicator}]"
                )

    return "\n".join(lines)
