"""Executable reference implementation of ``sp_BalancePerCustomer`` on SQLite.

What this proves, and what it does not:

* It DOES prove that the PySpark conversion produces the same rows and the same exact
  decimal values as a straight transliteration of the T-SQL body, over the same fixture
  data, including the behaviours that differ between the two engines: case-insensitive
  ``LIKE``/equality, ``%``/``_`` wildcards inside the parameter, ``ISNULL`` on a
  non-matching account, negative balances, and fixed-point money arithmetic.
* It does NOT prove SQL Server behaviour that SQLite cannot host: real collation rules
  beyond ASCII case folding, ``LIKE`` character classes (``[a-z]``), ``MONEY`` overflow at
  +/-922 337 203 685 477.5807, or the query plans/index behaviour of either engine.

Money is modelled the way SQL Server stores ``MONEY``: a 64-bit integer of ten-thousandths
of a currency unit (documented fixed scale of 4). SQLite integer arithmetic is therefore
exact, and results are converted back to ``Decimal`` with scale 4 for comparison — no
floating point is involved on either side.

Case-insensitive comparison is modelled with ``COLLATE NOCASE`` on the text columns, which
mirrors SQL Server's default ``SQL_Latin1_General_CP1_CI_AS`` collation for ASCII.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Iterable, NamedTuple

SCALE = 10_000


class BalanceRow(NamedTuple):
    CustomerName: str
    AccountType: str
    InitialBalance: Decimal
    CurrentBalance: Decimal


DDL = """
CREATE TABLE DimCustomer (
    CustomerID   INTEGER PRIMARY KEY,
    CustomerName TEXT COLLATE NOCASE
);
CREATE TABLE DimAccount (
    AccountID   INTEGER PRIMARY KEY,
    CustomerID  INTEGER,
    AccountType TEXT COLLATE NOCASE,
    Balance     INTEGER,          -- MONEY as ten-thousandths
    Status      TEXT COLLATE NOCASE
);
CREATE TABLE FactTransaction (
    TransactionID   INTEGER PRIMARY KEY,
    AccountID       INTEGER,
    Amount          INTEGER,      -- MONEY as ten-thousandths
    TransactionType TEXT COLLATE NOCASE
);
"""

# Transliteration of sql_scripts/02_create_procedures.sql lines 58-89.
QUERY = """
WITH TransactionSummary AS (
    SELECT
        AccountID,
        SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
            AS TotalTransactionAmount
    FROM FactTransaction
    GROUP BY AccountID
)
SELECT
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    a.Balance + IFNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
FROM DimCustomer c
JOIN DimAccount a ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
WHERE c.CustomerName LIKE '%' || ? || '%'
  AND a.Status = 'active';
"""


def to_money(value: Any) -> int:
    return int((Decimal(str(value)) * SCALE).to_integral_value())


def from_money(value: int) -> Decimal:
    return (Decimal(value) / SCALE).quantize(Decimal("0.0001"))


def build_database(
    customers: Iterable[dict], accounts: Iterable[dict], transactions: Iterable[dict]
) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(DDL)
    connection.executemany(
        "INSERT INTO DimCustomer VALUES (?, ?)",
        [(c["CustomerID"], c["CustomerName"]) for c in customers],
    )
    connection.executemany(
        "INSERT INTO DimAccount VALUES (?, ?, ?, ?, ?)",
        [
            (a["AccountID"], a["CustomerID"], a["AccountType"], to_money(a["Balance"]), a["Status"])
            for a in accounts
        ],
    )
    connection.executemany(
        "INSERT INTO FactTransaction VALUES (?, ?, ?, ?)",
        [
            (t["TransactionID"], t["AccountID"], to_money(t["Amount"]), t["TransactionType"])
            for t in transactions
        ],
    )
    connection.commit()
    return connection


def sp_balance_per_customer(
    connection: sqlite3.Connection, customer_name: str
) -> list[BalanceRow]:
    rows = connection.execute(QUERY, (customer_name,)).fetchall()
    return [
        BalanceRow(name, account_type, from_money(initial), from_money(current))
        for name, account_type, initial, current in rows
    ]
