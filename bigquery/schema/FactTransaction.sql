-- BigQuery DDL for FactTransaction
-- Migrated from SQL Server T-SQL schema

CREATE TABLE IF NOT EXISTS FactTransaction (
    TransactionID   INT64 NOT NULL,
    AccountID       INT64,
    TransactionDate DATETIME,
    Amount          NUMERIC,
    TransactionType STRING,
    BranchID        INT64,

    PRIMARY KEY (TransactionID) NOT ENFORCED,

    CONSTRAINT FK_FactTransaction_DimAccount
        FOREIGN KEY (AccountID) REFERENCES DimAccount(AccountID) NOT ENFORCED,
    CONSTRAINT FK_FactTransaction_DimBranch
        FOREIGN KEY (BranchID) REFERENCES DimBranch(BranchID) NOT ENFORCED
);
