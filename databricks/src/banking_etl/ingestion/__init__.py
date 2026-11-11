"""Bronze-layer source ingestion for the banking warehouse.

Sources (see ``sources.py``):

* the six operational tables restored from ``data_sources/sample.bak``
  (``account``, ``branch``, ``city``, ``customer``, ``state``,
  ``transaction_db``), read live over JDBC / Lakehouse Federation or, as a
  fallback, from CSV extracts landed in a volume;
* ``transaction_csv.csv`` and ``transaction_excel.xlsx`` file drops.

Every source lands in ``<catalog>.<bronze_schema>.<source_name>`` as an
append-only Delta table carrying the ingestion metadata columns listed in
``metadata.METADATA_COLUMNS``.
"""
