"""
Schema Comparison Validation
Compares table schemas (column names, types) between legacy and Databricks.
"""

import pandas as pd


# Expected schemas for the DWH star schema
EXPECTED_SCHEMAS = {
    "DimAccount": {
        "AccountID": "int",
        "CustomerID": "int",
        "AccountType": "string",
        "Balance": "numeric",
        "DateOpened": "date",
        "Status": "string",
    },
    "DimBranch": {
        "BranchID": "int",
        "BranchName": "string",
        "BranchLocation": "string",
    },
    "DimCustomer": {
        "CustomerID": "int",
        "CustomerName": "string",
        "Address": "string",
        "CityName": "string",
        "StateName": "string",
        "Age": "int",
        "Gender": "string",
        "Email": "string",
    },
    "FactTransaction": {
        "TransactionID": "int",
        "AccountID": "int",
        "TransactionDate": "datetime",
        "Amount": "numeric",
        "TransactionType": "string",
        "BranchID": "int",
    },
}


def _normalize_dtype(dtype_str: str) -> str:
    """Map a pandas/SQL dtype to a generic category."""
    dtype_lower = str(dtype_str).lower()

    if "int" in dtype_lower:
        return "int"
    if "float" in dtype_lower or "money" in dtype_lower or "decimal" in dtype_lower or "numeric" in dtype_lower:
        return "numeric"
    if "datetime" in dtype_lower or "timestamp" in dtype_lower:
        return "datetime"
    if "date" in dtype_lower:
        return "date"
    if "object" in dtype_lower or "str" in dtype_lower or "varchar" in dtype_lower or "string" in dtype_lower:
        return "string"
    if "bool" in dtype_lower:
        return "boolean"

    return dtype_lower


def compare_schemas_from_dataframes(
    legacy_tables: dict[str, pd.DataFrame],
    databricks_tables: dict[str, pd.DataFrame],
) -> list[dict]:
    """Compare schemas between legacy and Databricks DataFrames."""
    results = []
    all_tables = sorted(set(list(legacy_tables.keys()) + list(databricks_tables.keys())))

    for table in all_tables:
        legacy_df = legacy_tables.get(table)
        db_df = databricks_tables.get(table)

        if legacy_df is None:
            results.append({
                "table": table,
                "status": "MISSING_IN_LEGACY",
                "column_results": [],
            })
            continue
        if db_df is None:
            results.append({
                "table": table,
                "status": "MISSING_IN_DATABRICKS",
                "column_results": [],
            })
            continue

        legacy_cols = {col: _normalize_dtype(legacy_df[col].dtype) for col in legacy_df.columns}
        db_cols = {col: _normalize_dtype(db_df[col].dtype) for col in db_df.columns}
        all_cols = sorted(set(list(legacy_cols.keys()) + list(db_cols.keys())))

        column_results = []
        all_match = True

        for col in all_cols:
            l_type = legacy_cols.get(col)
            d_type = db_cols.get(col)

            if l_type is None:
                status = "MISSING_IN_LEGACY"
                all_match = False
            elif d_type is None:
                status = "MISSING_IN_DATABRICKS"
                all_match = False
            elif l_type == d_type:
                status = "PASS"
            else:
                # Check for compatible type promotions
                if _types_compatible(l_type, d_type):
                    status = "COMPATIBLE"
                else:
                    status = "FAIL"
                    all_match = False

            column_results.append({
                "column": col,
                "legacy_type": l_type,
                "databricks_type": d_type,
                "status": status,
            })

        results.append({
            "table": table,
            "status": "PASS" if all_match else "FAIL",
            "column_results": column_results,
        })

    return results


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Check if two normalized types are compatible (e.g., int and numeric)."""
    compatible_groups = [
        {"int", "numeric"},
        {"date", "datetime"},
        {"string", "date"},
        {"string", "datetime"},
    ]
    for group in compatible_groups:
        if type_a in group and type_b in group:
            return True
    return False


def validate_against_expected_schema(
    tables: dict[str, pd.DataFrame],
    source_label: str = "source",
) -> list[dict]:
    """Validate DataFrames against the expected DWH schema definition."""
    results = []

    for table_name, expected_cols in EXPECTED_SCHEMAS.items():
        if table_name not in tables:
            results.append({
                "table": table_name,
                "source": source_label,
                "status": "MISSING",
                "column_results": [],
            })
            continue

        df = tables[table_name]
        actual_cols = {col: _normalize_dtype(df[col].dtype) for col in df.columns}
        column_results = []
        all_match = True

        for col, expected_type in expected_cols.items():
            actual_type = actual_cols.get(col)
            if actual_type is None:
                column_results.append({
                    "column": col,
                    "expected_type": expected_type,
                    "actual_type": None,
                    "status": "MISSING",
                })
                all_match = False
            elif actual_type == expected_type or _types_compatible(actual_type, expected_type):
                column_results.append({
                    "column": col,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                    "status": "PASS",
                })
            else:
                column_results.append({
                    "column": col,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                    "status": "FAIL",
                })
                all_match = False

        # Check for extra columns not in expected schema
        for col in actual_cols:
            if col not in expected_cols:
                column_results.append({
                    "column": col,
                    "expected_type": None,
                    "actual_type": actual_cols[col],
                    "status": "EXTRA",
                })

        results.append({
            "table": table_name,
            "source": source_label,
            "status": "PASS" if all_match else "FAIL",
            "column_results": column_results,
        })

    return results


def format_schema_results(results: list[dict]) -> str:
    """Format schema comparison results as a human-readable report."""
    lines = []

    for r in results:
        lines.append(f"\n  Table: {r['table']} - {r['status']}")
        for c in r.get("column_results", []):
            l_type = c.get("legacy_type") or c.get("expected_type") or "N/A"
            d_type = c.get("databricks_type") or c.get("actual_type") or "N/A"
            lines.append(f"    {c['column']:<25} {l_type:<15} {d_type:<15} [{c['status']}]")

    return "\n".join(lines)
