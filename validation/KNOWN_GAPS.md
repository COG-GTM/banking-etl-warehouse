# Known Gaps: Legacy SQL Server DWH vs Databricks Target

This document catalogs all intentional deviations between the legacy SQL Server Data
Warehouse implementation and the Databricks Bronze/Silver/Gold medallion target state.

Each gap is classified as one of:
- **Cosmetic**: Display or formatting differences with no data impact.
- **Behavioral**: Differences in logic or processing that may produce different results
  under specific conditions but do not affect correctness for standard use cases.
- **Data-affecting**: Differences that change actual data values or row counts.

---

## Gap Registry

### GAP-001: MONEY Type to DOUBLE/DECIMAL Precision

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Cosmetic                                                           |
| Tables      | DimAccount.Balance, FactTransaction.Amount                         |
| Description | SQL Server `MONEY` type stores values with 4 decimal places of     |
|             | precision. Databricks uses `DOUBLE` or `DECIMAL(19,4)` depending   |
|             | on the Gold layer implementation.                                  |
| Impact      | Possible rounding differences at the 4th+ decimal place. Banking   |
|             | amounts in this dataset use whole numbers or 2-decimal precision,   |
|             | so no practical impact.                                            |
| Mitigation  | Validation tolerances set to 0.01 for all numeric comparisons.     |
|             | If exact MONEY semantics are required, use `DECIMAL(19,4)` in      |
|             | Databricks.                                                        |

---

### GAP-002: DATETIME vs TIMESTAMP Handling

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Cosmetic                                                           |
| Tables      | FactTransaction.TransactionDate                                    |
| Description | SQL Server `DATETIME` has ~3.33ms precision. Databricks            |
|             | `TIMESTAMP` has microsecond precision. String representations may   |
|             | differ (e.g., `2024-01-18 09:00:00.000` vs `2024-01-18T09:00:00`). |
| Impact      | No data impact for this dataset since all timestamps are at second  |
|             | granularity. Display format differences may cause string comparison |
|             | failures.                                                          |
| Mitigation  | Comparisons cast both sides to date or truncate to seconds.        |

---

### GAP-003: NULL Handling in ISNULL vs COALESCE

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Behavioral                                                         |
| Tables      | sp_BalancePerCustomer output                                       |
| Description | Legacy uses `ISNULL(ts.TotalTransactionAmount, 0)`. Databricks     |
|             | equivalent uses `COALESCE(ts.TotalTransactionAmount, 0)`.          |
|             | `ISNULL` returns the data type of the first argument; `COALESCE`   |
|             | returns the highest-precedence type.                               |
| Impact      | No practical difference for this use case since both operate on    |
|             | numeric types and the fallback is `0`. Could matter if types were  |
|             | mixed.                                                             |
| Mitigation  | None needed. Both functions produce identical results here.        |

---

### GAP-004: LIKE Pattern Matching Case Sensitivity

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Behavioral                                                         |
| Tables      | sp_BalancePerCustomer (CustomerName filter)                        |
| Description | SQL Server `LIKE` is case-insensitive by default (depends on       |
|             | collation, typically `SQL_Latin1_General_CP1_CI_AS`). Databricks   |
|             | `LIKE` is case-sensitive by default.                               |
| Impact      | Searching for `'John'` would match `'JOHN SMITH'` in SQL Server   |
|             | but not in Databricks unless `ILIKE` or `LOWER()` is used.        |
| Mitigation  | Databricks implementation should use `ILIKE` or                    |
|             | `LOWER(CustomerName) LIKE LOWER('%' || param || '%')` to match    |
|             | legacy behavior. Validation scripts use case-insensitive matching. |

---

### GAP-005: Date Casting in WHERE Clause

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Behavioral                                                         |
| Tables      | sp_DailyTransaction (TransactionDate filter)                       |
| Description | Legacy uses `CAST(TransactionDate AS DATE)` to strip time from     |
|             | DATETIME before comparison. Databricks may handle this differently |
|             | depending on whether the column is stored as `TIMESTAMP` or `DATE`. |
| Impact      | If Databricks stores as `TIMESTAMP`, must also cast to `DATE` for  |
|             | correct range filtering. Otherwise, time components could cause    |
|             | boundary transactions (at midnight) to be included/excluded        |
|             | differently.                                                       |
| Mitigation  | Databricks queries should explicitly `CAST(TransactionDate AS DATE)` |
|             | to match legacy behavior.                                          |

---

### GAP-006: VARCHAR Length Constraints

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Cosmetic                                                           |
| Tables      | All dimension tables                                               |
| Description | SQL Server enforces `VARCHAR(N)` length limits at insert time.     |
|             | Databricks `STRING` type has no length constraint by default.      |
| Impact      | No data impact for migration (data already fits legacy constraints). |
|             | Future inserts into Databricks could accept longer values than     |
|             | the legacy system would.                                           |
| Mitigation  | Add CHECK constraints or Delta Lake column constraints if strict   |
|             | length enforcement is required.                                    |

---

### GAP-007: Sort Order for Tied Values

| Attribute   | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Category    | Cosmetic                                                           |
| Tables      | sp_DailyTransaction, sp_BalancePerCustomer outputs                 |
| Description | When multiple rows have the same sort key, SQL Server and          |
|             | Databricks may return them in different orders (non-deterministic). |
| Impact      | Row-by-row comparison may show false mismatches for rows that are  |
|             | otherwise identical in content.                                    |
| Mitigation  | Validation scripts sort results by all relevant columns before     |
|             | comparison.                                                        |

---

## Gap Summary

| Gap ID  | Category       | Severity | Requires Action |
|---------|---------------|----------|-----------------|
| GAP-001 | Cosmetic       | Low      | No              |
| GAP-002 | Cosmetic       | Low      | No              |
| GAP-003 | Behavioral     | Low      | No              |
| GAP-004 | Behavioral     | Medium   | Yes (use ILIKE) |
| GAP-005 | Behavioral     | Medium   | Yes (cast date) |
| GAP-006 | Cosmetic       | Low      | Optional        |
| GAP-007 | Cosmetic       | Low      | No              |

---

## Adding New Gaps

When a new deviation is discovered, add an entry following the template:

```markdown
### GAP-NNN: Short Description

| Attribute   | Value                     |
|-------------|---------------------------|
| Category    | Cosmetic / Behavioral / Data-affecting |
| Tables      | Affected tables/columns   |
| Description | What differs and why      |
| Impact      | Business impact           |
| Mitigation  | How to resolve or accept  |
```

Update the Gap Summary table accordingly.
