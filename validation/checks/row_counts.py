"""
Row Count Validation
Compares record counts for each dimension and fact table between legacy and Databricks.
"""

import pandas as pd


def check_row_counts_from_dataframes(
    legacy_tables: dict[str, pd.DataFrame],
    databricks_tables: dict[str, pd.DataFrame],
) -> list[dict]:
    """Compare row counts between legacy and Databricks DataFrames.

    Args:
        legacy_tables: Dict mapping table name -> DataFrame from legacy system.
        databricks_tables: Dict mapping table name -> DataFrame from Databricks.

    Returns:
        List of result dicts with table, legacy_count, databricks_count, match status.
    """
    results = []
    all_tables = sorted(set(list(legacy_tables.keys()) + list(databricks_tables.keys())))

    for table in all_tables:
        legacy_count = len(legacy_tables[table]) if table in legacy_tables else None
        db_count = len(databricks_tables[table]) if table in databricks_tables else None

        if legacy_count is None:
            status = "MISSING_IN_LEGACY"
        elif db_count is None:
            status = "MISSING_IN_DATABRICKS"
        elif legacy_count == db_count:
            status = "PASS"
        else:
            status = "FAIL"

        results.append({
            "table": table,
            "legacy_count": legacy_count,
            "databricks_count": db_count,
            "difference": (db_count or 0) - (legacy_count or 0),
            "status": status,
        })

    return results


def check_row_counts_from_connections(
    legacy_conn,
    databricks_conn,
    tables: list[str],
) -> list[dict]:
    """Compare row counts using live database connections.

    Args:
        legacy_conn: SQL Server connection (pyodbc or similar).
        databricks_conn: Databricks SQL connection.
        tables: List of table names to compare.

    Returns:
        List of result dicts.
    """
    results = []

    for table in tables:
        legacy_count = None
        db_count = None

        try:
            legacy_df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table}", legacy_conn)
            legacy_count = int(legacy_df["cnt"].iloc[0])
        except Exception as e:
            legacy_count = f"ERROR: {e}"

        try:
            db_df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table}", databricks_conn)
            db_count = int(db_df["cnt"].iloc[0])
        except Exception as e:
            db_count = f"ERROR: {e}"

        if isinstance(legacy_count, str) or isinstance(db_count, str):
            status = "ERROR"
            difference = None
        elif legacy_count == db_count:
            status = "PASS"
            difference = 0
        else:
            status = "FAIL"
            difference = db_count - legacy_count

        results.append({
            "table": table,
            "legacy_count": legacy_count,
            "databricks_count": db_count,
            "difference": difference,
            "status": status,
        })

    return results


def format_row_count_results(results: list[dict]) -> str:
    """Format row count results as a human-readable table."""
    lines = []
    lines.append(f"{'Table':<25} {'Legacy':>10} {'Databricks':>12} {'Diff':>8} {'Status':>10}")
    lines.append("-" * 70)

    for r in results:
        legacy = str(r["legacy_count"]) if r["legacy_count"] is not None else "N/A"
        db = str(r["databricks_count"]) if r["databricks_count"] is not None else "N/A"
        diff = str(r["difference"]) if r["difference"] is not None else "N/A"
        lines.append(f"{r['table']:<25} {legacy:>10} {db:>12} {diff:>8} {r['status']:>10}")

    return "\n".join(lines)
