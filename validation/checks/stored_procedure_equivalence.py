"""
Stored Procedure Equivalence Validation
Compares outputs of sp_DailyTransaction and sp_BalancePerCustomer
between legacy SQL Server and Databricks implementations.
"""

import pandas as pd


# ---------------------------------------------------------------------------
# sp_DailyTransaction equivalence
# ---------------------------------------------------------------------------

def compute_daily_transactions(
    fact_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    date_col: str = "TransactionDate",
    amount_col: str = "Amount",
    id_col: str = "TransactionID",
) -> pd.DataFrame:
    """Replicate sp_DailyTransaction logic in pandas.

    Aggregates transaction count and total amount per day within a date range.
    Mirrors the SQL:
        SELECT CAST(TransactionDate AS DATE) AS [Date],
               COUNT(TransactionID) AS TotalTransactions,
               SUM(Amount) AS TotalAmount
        FROM FactTransaction
        WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
        GROUP BY CAST(TransactionDate AS DATE)
        ORDER BY [Date];
    """
    df = fact_df.copy()
    df["_date"] = pd.to_datetime(df[date_col]).dt.date
    start = pd.to_datetime(start_date).date()
    end = pd.to_datetime(end_date).date()

    filtered = df[(df["_date"] >= start) & (df["_date"] <= end)]

    result = (
        filtered.groupby("_date")
        .agg(
            TotalTransactions=(id_col, "count"),
            TotalAmount=(amount_col, "sum"),
        )
        .reset_index()
        .rename(columns={"_date": "Date"})
        .sort_values("Date")
        .reset_index(drop=True)
    )

    return result


def compare_daily_transactions(
    legacy_fact: pd.DataFrame,
    databricks_fact: pd.DataFrame,
    start_date: str,
    end_date: str,
    tolerance: float = 0.01,
) -> dict:
    """Compare sp_DailyTransaction outputs between legacy and Databricks data."""
    legacy_result = compute_daily_transactions(legacy_fact, start_date, end_date)
    db_result = compute_daily_transactions(databricks_fact, start_date, end_date)

    row_match = len(legacy_result) == len(db_result)
    field_results = []

    all_dates = sorted(
        set(legacy_result["Date"].tolist()) | set(db_result["Date"].tolist())
    )

    for d in all_dates:
        legacy_row = legacy_result[legacy_result["Date"] == d]
        db_row = db_result[db_result["Date"] == d]

        if legacy_row.empty:
            field_results.append({
                "date": str(d),
                "status": "MISSING_IN_LEGACY",
                "legacy_count": None,
                "databricks_count": None,
                "legacy_amount": None,
                "databricks_amount": None,
            })
            continue
        if db_row.empty:
            field_results.append({
                "date": str(d),
                "status": "MISSING_IN_DATABRICKS",
                "legacy_count": None,
                "databricks_count": None,
                "legacy_amount": None,
                "databricks_amount": None,
            })
            continue

        l_count = int(legacy_row["TotalTransactions"].iloc[0])
        d_count = int(db_row["TotalTransactions"].iloc[0])
        l_amount = float(legacy_row["TotalAmount"].iloc[0])
        d_amount = float(db_row["TotalAmount"].iloc[0])

        count_match = l_count == d_count
        amount_match = abs(l_amount - d_amount) <= tolerance

        field_results.append({
            "date": str(d),
            "status": "PASS" if (count_match and amount_match) else "FAIL",
            "legacy_count": l_count,
            "databricks_count": d_count,
            "legacy_amount": l_amount,
            "databricks_amount": d_amount,
        })

    all_pass = row_match and all(r["status"] == "PASS" for r in field_results)

    return {
        "procedure": "sp_DailyTransaction",
        "parameters": {"start_date": start_date, "end_date": end_date},
        "status": "PASS" if all_pass else "FAIL",
        "row_count_match": row_match,
        "detail": field_results,
    }


# ---------------------------------------------------------------------------
# sp_BalancePerCustomer equivalence
# ---------------------------------------------------------------------------

def compute_balance_per_customer(
    fact_df: pd.DataFrame,
    account_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    customer_name: str,
    type_col: str = "TransactionType",
    amount_col: str = "Amount",
) -> pd.DataFrame:
    """Replicate sp_BalancePerCustomer logic in pandas.

    Mirrors the SQL CTE:
        WITH TransactionSummary AS (
            SELECT AccountID,
                   SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
                       AS TotalTransactionAmount
            FROM FactTransaction
            GROUP BY AccountID
        )
        SELECT c.CustomerName, a.AccountType, a.Balance AS InitialBalance,
               a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
        FROM DimCustomer c
        JOIN DimAccount a ON c.CustomerID = a.CustomerID
        LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
        WHERE c.CustomerName LIKE '%' + @customer_name + '%'
              AND a.Status = 'active';
    """
    # Build TransactionSummary CTE
    fact = fact_df.copy()
    fact["_signed_amount"] = fact.apply(
        lambda row: row[amount_col] if row[type_col] == "Deposit" else -row[amount_col],
        axis=1,
    )
    txn_summary = (
        fact.groupby("AccountID")["_signed_amount"]
        .sum()
        .reset_index()
        .rename(columns={"_signed_amount": "TotalTransactionAmount"})
    )

    # Join tables
    merged = customer_df.merge(account_df, on="CustomerID", how="inner")
    merged = merged.merge(txn_summary, on="AccountID", how="left")
    merged["TotalTransactionAmount"] = merged["TotalTransactionAmount"].fillna(0)

    # Filter
    filtered = merged[
        merged["CustomerName"].str.contains(customer_name, case=False, na=False)
        & (merged["Status"] == "active")
    ].copy()

    filtered["CurrentBalance"] = filtered["Balance"] + filtered["TotalTransactionAmount"]

    result = filtered[["CustomerName", "AccountType", "Balance", "CurrentBalance"]].copy()
    result = result.rename(columns={"Balance": "InitialBalance"})
    result = result.sort_values(["CustomerName", "AccountType"]).reset_index(drop=True)

    return result


def compare_balance_per_customer(
    legacy_tables: dict[str, pd.DataFrame],
    databricks_tables: dict[str, pd.DataFrame],
    customer_name: str,
    tolerance: float = 0.01,
) -> dict:
    """Compare sp_BalancePerCustomer outputs between legacy and Databricks data."""
    legacy_result = compute_balance_per_customer(
        legacy_tables["FactTransaction"],
        legacy_tables["DimAccount"],
        legacy_tables["DimCustomer"],
        customer_name,
    )
    db_result = compute_balance_per_customer(
        databricks_tables["FactTransaction"],
        databricks_tables["DimAccount"],
        databricks_tables["DimCustomer"],
        customer_name,
    )

    row_match = len(legacy_result) == len(db_result)
    field_results = []

    # Align on CustomerName + AccountType for comparison
    legacy_keyed = legacy_result.set_index(["CustomerName", "AccountType"])
    db_keyed = db_result.set_index(["CustomerName", "AccountType"])
    all_keys = sorted(set(legacy_keyed.index) | set(db_keyed.index))

    for key in all_keys:
        cname, atype = key
        if key not in legacy_keyed.index:
            field_results.append({
                "customer_name": cname,
                "account_type": atype,
                "status": "MISSING_IN_LEGACY",
            })
            continue
        if key not in db_keyed.index:
            field_results.append({
                "customer_name": cname,
                "account_type": atype,
                "status": "MISSING_IN_DATABRICKS",
            })
            continue

        l_row = legacy_keyed.loc[key]
        d_row = db_keyed.loc[key]

        initial_match = abs(float(l_row["InitialBalance"]) - float(d_row["InitialBalance"])) <= tolerance
        current_match = abs(float(l_row["CurrentBalance"]) - float(d_row["CurrentBalance"])) <= tolerance

        field_results.append({
            "customer_name": cname,
            "account_type": atype,
            "legacy_initial_balance": float(l_row["InitialBalance"]),
            "databricks_initial_balance": float(d_row["InitialBalance"]),
            "legacy_current_balance": float(l_row["CurrentBalance"]),
            "databricks_current_balance": float(d_row["CurrentBalance"]),
            "status": "PASS" if (initial_match and current_match) else "FAIL",
        })

    all_pass = row_match and all(r["status"] == "PASS" for r in field_results)

    return {
        "procedure": "sp_BalancePerCustomer",
        "parameters": {"customer_name": customer_name},
        "status": "PASS" if all_pass else "FAIL",
        "row_count_match": row_match,
        "detail": field_results,
    }


def format_sp_results(result: dict) -> str:
    """Format stored procedure comparison results."""
    lines = []
    lines.append(f"\n  Procedure: {result['procedure']}")
    lines.append(f"  Parameters: {result['parameters']}")
    lines.append(f"  Overall Status: {result['status']}")
    lines.append(f"  Row Count Match: {result['row_count_match']}")
    lines.append("")

    for d in result["detail"]:
        if result["procedure"] == "sp_DailyTransaction":
            lines.append(
                f"    Date={d['date']}  "
                f"Count: {d.get('legacy_count', 'N/A')}/{d.get('databricks_count', 'N/A')}  "
                f"Amount: {d.get('legacy_amount', 'N/A')}/{d.get('databricks_amount', 'N/A')}  "
                f"[{d['status']}]"
            )
        else:
            lines.append(
                f"    Customer={d.get('customer_name', 'N/A')}  "
                f"Type={d.get('account_type', 'N/A')}  "
                f"[{d['status']}]"
            )
            if "legacy_initial_balance" in d:
                lines.append(
                    f"      InitialBalance: {d['legacy_initial_balance']:.2f} / "
                    f"{d['databricks_initial_balance']:.2f}  "
                    f"CurrentBalance: {d['legacy_current_balance']:.2f} / "
                    f"{d['databricks_current_balance']:.2f}"
                )

    return "\n".join(lines)
