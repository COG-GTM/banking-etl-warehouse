"""
Key Aggregate Validation
Compares aggregate metrics between legacy and Databricks:
- Sum of transaction amounts
- Count of transactions by type
- Distinct customer counts
- Account balance totals
- Branch transaction distributions
"""

import pandas as pd


def check_transaction_amount_totals(
    legacy_fact: pd.DataFrame,
    databricks_fact: pd.DataFrame,
    amount_col: str = "Amount",
    tolerance: float = 0.01,
) -> dict:
    """Compare total transaction amounts."""
    legacy_total = legacy_fact[amount_col].sum()
    db_total = databricks_fact[amount_col].sum()
    diff = abs(legacy_total - db_total)

    return {
        "check": "total_transaction_amount",
        "legacy_value": float(legacy_total),
        "databricks_value": float(db_total),
        "difference": float(diff),
        "status": "PASS" if diff <= tolerance else "FAIL",
    }


def check_transaction_counts_by_type(
    legacy_fact: pd.DataFrame,
    databricks_fact: pd.DataFrame,
    type_col: str = "TransactionType",
) -> list[dict]:
    """Compare transaction counts grouped by TransactionType."""
    legacy_counts = legacy_fact[type_col].value_counts().sort_index()
    db_counts = databricks_fact[type_col].value_counts().sort_index()

    all_types = sorted(set(legacy_counts.index) | set(db_counts.index))
    results = []

    for txn_type in all_types:
        legacy_val = int(legacy_counts.get(txn_type, 0))
        db_val = int(db_counts.get(txn_type, 0))
        results.append({
            "check": f"transaction_count_{txn_type}",
            "transaction_type": txn_type,
            "legacy_value": legacy_val,
            "databricks_value": db_val,
            "difference": db_val - legacy_val,
            "status": "PASS" if legacy_val == db_val else "FAIL",
        })

    return results


def check_distinct_customer_count(
    legacy_customers: pd.DataFrame,
    databricks_customers: pd.DataFrame,
    id_col: str = "CustomerID",
) -> dict:
    """Compare distinct customer counts."""
    legacy_count = legacy_customers[id_col].nunique()
    db_count = databricks_customers[id_col].nunique()

    return {
        "check": "distinct_customer_count",
        "legacy_value": int(legacy_count),
        "databricks_value": int(db_count),
        "difference": int(db_count - legacy_count),
        "status": "PASS" if legacy_count == db_count else "FAIL",
    }


def check_amount_sum_by_type(
    legacy_fact: pd.DataFrame,
    databricks_fact: pd.DataFrame,
    type_col: str = "TransactionType",
    amount_col: str = "Amount",
    tolerance: float = 0.01,
) -> list[dict]:
    """Compare sum of amounts grouped by TransactionType."""
    legacy_sums = legacy_fact.groupby(type_col)[amount_col].sum().sort_index()
    db_sums = databricks_fact.groupby(type_col)[amount_col].sum().sort_index()

    all_types = sorted(set(legacy_sums.index) | set(db_sums.index))
    results = []

    for txn_type in all_types:
        legacy_val = float(legacy_sums.get(txn_type, 0))
        db_val = float(db_sums.get(txn_type, 0))
        diff = abs(legacy_val - db_val)
        results.append({
            "check": f"amount_sum_{txn_type}",
            "transaction_type": txn_type,
            "legacy_value": legacy_val,
            "databricks_value": db_val,
            "difference": diff,
            "status": "PASS" if diff <= tolerance else "FAIL",
        })

    return results


def check_distinct_account_count(
    legacy_accounts: pd.DataFrame,
    databricks_accounts: pd.DataFrame,
    id_col: str = "AccountID",
) -> dict:
    """Compare distinct account counts."""
    legacy_count = legacy_accounts[id_col].nunique()
    db_count = databricks_accounts[id_col].nunique()

    return {
        "check": "distinct_account_count",
        "legacy_value": int(legacy_count),
        "databricks_value": int(db_count),
        "difference": int(db_count - legacy_count),
        "status": "PASS" if legacy_count == db_count else "FAIL",
    }


def check_branch_transaction_distribution(
    legacy_fact: pd.DataFrame,
    databricks_fact: pd.DataFrame,
    branch_col: str = "BranchID",
) -> list[dict]:
    """Compare transaction counts per branch."""
    legacy_counts = legacy_fact[branch_col].value_counts().sort_index()
    db_counts = databricks_fact[branch_col].value_counts().sort_index()

    all_branches = sorted(set(legacy_counts.index) | set(db_counts.index))
    results = []

    for branch in all_branches:
        legacy_val = int(legacy_counts.get(branch, 0))
        db_val = int(db_counts.get(branch, 0))
        results.append({
            "check": f"branch_{branch}_transaction_count",
            "branch_id": branch,
            "legacy_value": legacy_val,
            "databricks_value": db_val,
            "difference": db_val - legacy_val,
            "status": "PASS" if legacy_val == db_val else "FAIL",
        })

    return results


def run_all_aggregate_checks(
    legacy_tables: dict[str, pd.DataFrame],
    databricks_tables: dict[str, pd.DataFrame],
) -> list[dict]:
    """Run all aggregate checks and return combined results."""
    results = []

    if "FactTransaction" in legacy_tables and "FactTransaction" in databricks_tables:
        legacy_fact = legacy_tables["FactTransaction"]
        db_fact = databricks_tables["FactTransaction"]

        results.append(check_transaction_amount_totals(legacy_fact, db_fact))
        results.extend(check_transaction_counts_by_type(legacy_fact, db_fact))
        results.extend(check_amount_sum_by_type(legacy_fact, db_fact))
        results.extend(check_branch_transaction_distribution(legacy_fact, db_fact))

    if "DimCustomer" in legacy_tables and "DimCustomer" in databricks_tables:
        results.append(check_distinct_customer_count(
            legacy_tables["DimCustomer"],
            databricks_tables["DimCustomer"],
        ))

    if "DimAccount" in legacy_tables and "DimAccount" in databricks_tables:
        results.append(check_distinct_account_count(
            legacy_tables["DimAccount"],
            databricks_tables["DimAccount"],
        ))

    return results


def format_aggregate_results(results: list[dict]) -> str:
    """Format aggregate results as a human-readable table."""
    lines = []
    lines.append(f"{'Check':<40} {'Legacy':>14} {'Databricks':>14} {'Diff':>14} {'Status':>8}")
    lines.append("-" * 95)

    for r in results:
        legacy = f"{r['legacy_value']:.2f}" if isinstance(r["legacy_value"], float) else str(r["legacy_value"])
        db = f"{r['databricks_value']:.2f}" if isinstance(r["databricks_value"], float) else str(r["databricks_value"])
        diff = f"{r['difference']:.2f}" if isinstance(r["difference"], float) else str(r["difference"])
        lines.append(f"{r['check']:<40} {legacy:>14} {db:>14} {diff:>14} {r['status']:>8}")

    return "\n".join(lines)
