"""Legacy SQL Server DWH vs Delta gold reconciliation harness.

Checks per table: schema, row count, PK uniqueness, FK orphans, column checksums, a
whole-row checksum, business aggregates (SUM(amount) by type, min/max dates, ...), and a full
row-level diff keyed on the PK. Stored-procedure outputs and Talend reject parity are
compared too. Every check (and every mismatching row, up to a cap) becomes a row in
``ops.reconciliation_results``; the cutover gate is green only when every check passes.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banking_etl.validation import specs
from banking_etl.validation.tables import write_table

NULL_TOKEN = "<NULL>"
PASS, FAIL = "PASS", "FAIL"

RESULT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("run_ts", T.TimestampType(), False),
    T.StructField("run_label", T.StringType()),
    T.StructField("record_type", T.StringType(), False),  # check | mismatch
    T.StructField("table_name", T.StringType()),
    T.StructField("check_type", T.StringType()),
    T.StructField("check_name", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("legacy_value", T.StringType()),
    T.StructField("target_value", T.StringType()),
    T.StructField("mismatch_count", T.LongType()),
    T.StructField("pk_value", T.StringType()),
    T.StructField("column_name", T.StringType()),
    T.StructField("details", T.StringType()),
])


@dataclass
class ReconciliationReport:
    run_id: str
    run_ts: dt.datetime
    run_label: str
    rows: list[dict] = field(default_factory=list)

    @property
    def checks(self) -> list[dict]:
        return [r for r in self.rows if r["record_type"] == "check"]

    @property
    def mismatches(self) -> list[dict]:
        return [r for r in self.rows if r["record_type"] == "mismatch"]

    @property
    def failed(self) -> list[dict]:
        return [r for r in self.checks if r["status"] != PASS]

    @property
    def match_pct(self) -> float:
        total = len(self.checks)
        return 100.0 * (total - len(self.failed)) / total if total else 0.0

    @property
    def green(self) -> bool:
        return bool(self.checks) and not self.failed

    def summary(self) -> dict:
        by_type: dict[str, dict[str, int]] = {}
        for r in self.checks:
            t = by_type.setdefault(r["check_type"], {PASS: 0, FAIL: 0})
            t[r["status"]] += 1
        return {"run_id": self.run_id, "run_label": self.run_label, "green": self.green,
                "checks": len(self.checks), "failed": len(self.failed),
                "mismatch_rows": len(self.mismatches), "match_pct": round(self.match_pct, 2),
                "by_check_type": by_type}

    def to_df(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame([tuple(r[f.name] for f in RESULT_SCHEMA) for r in self.rows], RESULT_SCHEMA)

    def write(self, spark: SparkSession, table: str) -> None:
        write_table(self.to_df(spark), table, mode="append")

    # -- builders --
    def check(self, table, check_type, check_name, ok, legacy=None, target=None, mismatch_count=0, details=None):
        self._add("check", table, check_type, check_name, PASS if ok else FAIL, legacy, target,
                  mismatch_count, None, None, details)

    def mismatch(self, table, check_type, pk_value, column, legacy, target, details=None):
        self._add("mismatch", table, check_type, None, FAIL, legacy, target, 1, pk_value, column, details)

    def _add(self, record_type, table, check_type, check_name, status, legacy, target, mismatch_count,
             pk_value, column, details):
        self.rows.append({
            "run_id": self.run_id, "run_ts": self.run_ts, "run_label": self.run_label,
            "record_type": record_type, "table_name": table, "check_type": check_type,
            "check_name": check_name, "status": status, "legacy_value": _s(legacy),
            "target_value": _s(target), "mismatch_count": int(mismatch_count), "pk_value": pk_value,
            "column_name": column,
            "details": details if details is None or isinstance(details, str) else json.dumps(details, default=str),
        })


def _s(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str, sort_keys=True)
    return str(v)


def canonical(col: SparkColumn, dtype: T.DataType) -> SparkColumn:
    """Engine-neutral string form used for checksums and the row diff."""
    if isinstance(dtype, T.DecimalType):
        c = col.cast(T.DecimalType(38, 4)).cast("string")
    elif isinstance(dtype, T.TimestampType):
        c = F.date_format(col, "yyyy-MM-dd HH:mm:ss.SSS")
    elif isinstance(dtype, T.DateType):
        c = F.date_format(col, "yyyy-MM-dd")
    else:
        c = col.cast("string")
    return F.coalesce(c, F.lit(NULL_TOKEN))


def _conform(df: DataFrame, spec: specs.TableSpec) -> DataFrame:
    """Project onto the spec's gold columns/types (missing columns become NULL)."""
    present = set(df.columns)
    return df.select([
        (F.col(c.gold).cast(c.dtype) if c.gold in present else F.lit(None).cast(c.dtype)).alias(c.gold)
        for c in spec.columns
    ])


def _pk_key(spec: specs.TableSpec) -> SparkColumn:
    dtypes = {c.gold: c.dtype for c in spec.columns}
    return F.concat_ws("|", *[canonical(F.col(p), dtypes[p]) for p in spec.pk])


# ---------------------------------------------------------------- table checks
def _schema_check(rep, spec, target_raw: DataFrame):
    actual = {f.name: f.dataType.simpleString() for f in target_raw.schema.fields}
    missing = [c for c in spec.gold_columns if c not in actual]
    extra = [c for c in actual if c not in spec.gold_columns]
    expected = {c.gold: c.dtype.simpleString() for c in spec.columns}
    type_diffs = {c: {"expected": expected[c], "actual": actual[c]}
                  for c in expected if c in actual and actual[c] != expected[c]}
    rep.check(spec.gold, "schema", "columns_present", not missing, legacy=spec.gold_columns,
              target=list(actual), mismatch_count=len(missing),
              details={"missing": missing, "extra": extra, "type_differences": type_diffs})


def _row_count(rep, spec, legacy, target):
    lc, tc = legacy.count(), target.count()
    rep.check(spec.gold, "row_count", "row_count", lc == tc, lc, tc, abs(lc - tc))


def _pk_unique(rep, spec, side: str, df: DataFrame):
    pk_null = F.lit(False)
    for p in spec.pk:
        pk_null = pk_null | F.col(p).isNull()
    r = df.agg(F.count(F.lit(1)).alias("n"),
               F.countDistinct(*[F.col(p) for p in spec.pk]).alias("d"),
               F.sum(F.when(pk_null, 1).otherwise(0)).alias("nulls")).first()
    n, d, nulls = r["n"], r["d"], r["nulls"] or 0
    dupes = (n - nulls) - d
    rep.check(spec.gold, "pk_unique", f"pk_unique_{side}", dupes == 0 and nulls == 0,
              legacy=n if side == "legacy" else None, target=n if side == "target" else None,
              mismatch_count=dupes + nulls,
              details={"pk": list(spec.pk), "rows": n, "distinct": d, "null_pk_rows": nulls})
    if dupes:
        for row in (df.groupBy(*spec.pk).count().filter("count > 1").limit(50).collect()):
            rep.mismatch(spec.gold, f"pk_duplicate_{side}", "|".join(str(row[p]) for p in spec.pk),
                         None, None, row["count"])


def _orphans(child: DataFrame, fk: specs.ForeignKey, parent: DataFrame) -> DataFrame:
    p = parent.select(F.col(fk.ref_column).alias("_ref")).distinct()
    return child.filter(F.col(fk.column).isNotNull()).join(p, F.col(fk.column) == p._ref, "left_anti")


def _fk_checks(rep, spec, legacy_all, target_all):
    for fk in spec.fks:
        if fk.ref_table not in target_all:
            continue
        lo = _orphans(legacy_all[spec.gold], fk, legacy_all[fk.ref_table]).count()
        t_orph = _orphans(target_all[spec.gold], fk, target_all[fk.ref_table])
        to = t_orph.count()
        ok = to == 0 if fk.enforced_in_legacy else to == lo
        rep.check(spec.gold, "fk_orphans", f"{fk.column}->{fk.ref_table}.{fk.ref_column}", ok, lo, to, to,
                  details={"enforced_in_legacy": fk.enforced_in_legacy})
        if to and not ok:
            for row in t_orph.select(*spec.pk, fk.column).limit(50).collect():
                rep.mismatch(spec.gold, "fk_orphan", "|".join(str(row[p]) for p in spec.pk), fk.column,
                             None, row[fk.column], details={"ref": f"{fk.ref_table}.{fk.ref_column}"})


def _checksums(df: DataFrame, spec: specs.TableSpec) -> dict[str, tuple[int, int]]:
    aggs = []
    for c in spec.columns:
        aggs.append(F.sum(F.crc32(canonical(F.col(c.gold), c.dtype))).cast("long").alias(f"h__{c.gold}"))
        aggs.append(F.sum(F.when(F.col(c.gold).isNull(), 1).otherwise(0)).cast("long").alias(f"n__{c.gold}"))
    row_str = F.concat_ws("\u001f", *[canonical(F.col(c.gold), c.dtype) for c in spec.columns])
    aggs.append(F.sum(F.crc32(row_str)).cast("long").alias("h__<row>"))
    r = df.agg(*aggs).first()
    out = {c.gold: (r[f"h__{c.gold}"] or 0, r[f"n__{c.gold}"] or 0) for c in spec.columns}
    out["<row>"] = (r["h__<row>"] or 0, 0)
    return out


def _checksum_checks(rep, spec, legacy, target):
    lh, th = _checksums(legacy, spec), _checksums(target, spec)
    for col in [c.gold for c in spec.columns]:
        lv, tv = lh[col], th[col]
        rep.check(spec.gold, "column_checksum", col, lv == tv,
                  {"crc32_sum": lv[0], "nulls": lv[1]}, {"crc32_sum": tv[0], "nulls": tv[1]}, int(lv != tv))
    rep.check(spec.gold, "row_checksum", "all_columns", lh["<row>"] == th["<row>"],
              lh["<row>"][0], th["<row>"][0], int(lh["<row>"] != th["<row>"]))


def _aggregate(df: DataFrame, agg: specs.Aggregate) -> dict[str, str]:
    keys = [F.coalesce(F.col(g).cast("string"), F.lit(NULL_TOKEN)).alias(g) for g in agg.group_by]
    grouped = df.groupBy(*keys) if keys else df
    rows = grouped.agg(F.expr(agg.expr).alias("v")).collect()
    return {"|".join(str(r[g]) for g in agg.group_by) or "*": _s(r["v"]) for r in rows}


def _aggregate_checks(rep, spec, legacy, target):
    for agg in spec.aggregates:
        lv, tv = _aggregate(legacy, agg), _aggregate(target, agg)
        diff = sorted(k for k in set(lv) | set(tv) if lv.get(k) != tv.get(k))
        rep.check(spec.gold, "aggregate", agg.name, not diff, lv, tv, len(diff),
                  details={"expr": agg.expr, "group_by": list(agg.group_by), "differing_groups": diff})


def _row_diff(rep, spec, legacy, target, max_rows: int):
    value_cols = [c for c in spec.columns if c.gold not in spec.pk]

    def side(df, tag):
        return df.select(_pk_key(spec).alias("_pk"), F.lit(True).alias(f"_in_{tag}"),
                         *[canonical(F.col(c.gold), c.dtype).alias(f"{c.gold}__{tag}") for c in value_cols])

    j = side(legacy, "l").join(side(target, "t"), "_pk", "full_outer")
    diffs = F.filter(
        F.array(*[F.when(F.col(f"{c.gold}__l") != F.col(f"{c.gold}__t"),
                         F.struct(F.lit(c.gold).alias("col"), F.col(f"{c.gold}__l").alias("l"),
                                  F.col(f"{c.gold}__t").alias("t")))
                  for c in value_cols]),
        lambda x: x.isNotNull())
    j = j.withColumn("_kind", F.when(F.col("_in_t").isNull(), "missing_in_target")
                     .when(F.col("_in_l").isNull(), "missing_in_legacy")
                     .when(F.size(diffs) > 0, "value_mismatch")).filter(F.col("_kind").isNotNull())
    exploded = j.select("_pk", "_kind", F.explode_outer(
        F.when(F.col("_kind") == "value_mismatch", diffs)).alias("d"))
    counts = {r["_kind"]: r["count"] for r in exploded.groupBy("_kind").count().collect()}
    total = sum(counts.values())
    rep.check(spec.gold, "row_diff", "full_row_diff", total == 0, None, None, total, details=counts)
    for r in exploded.orderBy("_kind", "_pk").limit(max_rows).collect():
        d = r["d"]
        rep.mismatch(spec.gold, r["_kind"], r["_pk"], d["col"] if d else None,
                     d["l"] if d else None, d["t"] if d else None)


# ---------------------------------------------------------------- procs / rejects
def _rows(df: DataFrame, cols: list[str], dtypes: dict[str, T.DataType]) -> list[tuple]:
    return [tuple(r) for r in df.select([canonical(F.col(c), dtypes[c]).alias(c) for c in cols]).collect()]


def _proc_checks(rep, legacy_procs, target_procs):
    for proc, schema, params, ordered in (
        ("sp_DailyTransaction", specs.DAILY_TRANSACTION_SCHEMA, ["start_date", "end_date"], True),
        ("sp_BalancePerCustomer", specs.BALANCE_PER_CUSTOMER_SCHEMA, ["customer_name_param"], False),
    ):
        if proc not in target_procs:
            continue
        dtypes = {f.name: f.dataType for f in schema.fields}
        out_cols = [f.name for f in schema.fields if f.name not in params]
        cols = params + out_cols
        lrows, trows = _rows(legacy_procs[proc], cols, dtypes), _rows(target_procs[proc], cols, dtypes)
        param_sets = (specs.DAILY_TRANSACTION_PARAMS if ordered
                      else [(p,) for p in specs.BALANCE_PER_CUSTOMER_PARAMS])
        for ps in param_sets:
            key = tuple(str(p) for p in ps)
            lp = [r[len(params):] for r in lrows if r[:len(params)] == key]
            tp = [r[len(params):] for r in trows if r[:len(params)] == key]
            # Multiset compare: sp_DailyTransaction's ORDER BY [Date] key is unique per param set and
            # sp_BalancePerCustomer has no ORDER BY, so sorting both sides loses nothing.
            lp, tp = sorted(lp), sorted(tp)
            ok = lp == tp
            name = ",".join(f"{p}={v}" for p, v in zip(params, key))
            missing = [r for r in lp if r not in tp]
            extra = [r for r in tp if r not in lp]
            rep.check(proc, "proc_parity", name, ok, len(lp), len(tp), len(missing) + len(extra))
            for r in missing:
                rep.mismatch(proc, "proc_missing_in_target", name, None, dict(zip(out_cols, r)), None)
            for r in extra:
                rep.mismatch(proc, "proc_extra_in_target", name, None, None, dict(zip(out_cols, r)))


def _reject_check(rep, legacy_rejects: DataFrame, target_rejects: DataFrame):
    cols = ["transaction_id", "account_id", "branch_id", "source", "reason"]
    lv = sorted(tuple(r) for r in legacy_rejects.select(*cols).collect())
    tv = sorted(tuple(r) for r in target_rejects.select(*cols).collect())
    rep.check("fact_transaction", "reject_parity", "talend_rejects_vs_delta_rejects", lv == tv, len(lv), len(tv),
              len(set(lv) ^ set(tv)), details={"legacy": [list(x) for x in lv], "target": [list(x) for x in tv]})


# ---------------------------------------------------------------- entrypoint
def reconcile(
    spark: SparkSession,
    legacy: dict[str, DataFrame],
    target: dict[str, DataFrame],
    *,
    run_label: str = "adhoc",
    run_id: str | None = None,
    legacy_procs: dict[str, DataFrame] | None = None,
    target_procs: dict[str, DataFrame] | None = None,
    legacy_rejects: DataFrame | None = None,
    target_rejects: DataFrame | None = None,
    max_mismatch_rows: int = 1000,
    tables: tuple[specs.TableSpec, ...] = specs.GOLD_TABLES,
) -> ReconciliationReport:
    rep = ReconciliationReport(run_id or uuid.uuid4().hex, dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
                               run_label)
    leg = {s.gold: _conform(legacy[s.gold], s) for s in tables}
    tgt = {}
    for s in tables:
        if s.gold not in target:
            rep.check(s.gold, "schema", "table_present", False, details="target table missing")
            continue
        _schema_check(rep, s, target[s.gold])
        tgt[s.gold] = _conform(target[s.gold], s)
    for s in tables:
        if s.gold not in tgt:
            continue
        _row_count(rep, s, leg[s.gold], tgt[s.gold])
        _pk_unique(rep, s, "legacy", leg[s.gold])
        _pk_unique(rep, s, "target", tgt[s.gold])
        _fk_checks(rep, s, leg, tgt)
        _checksum_checks(rep, s, leg[s.gold], tgt[s.gold])
        _aggregate_checks(rep, s, leg[s.gold], tgt[s.gold])
        _row_diff(rep, s, leg[s.gold], tgt[s.gold], max_mismatch_rows)
    if legacy_procs is not None and target_procs is not None:
        _proc_checks(rep, legacy_procs, target_procs)
    if legacy_rejects is not None and target_rejects is not None:
        _reject_check(rep, legacy_rejects, target_rejects)
    return rep
