"""Ticket 7: Silver dim_customer (Talend Load_DimCustomer port)."""

import csv
import re
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.tables import (
    BRONZE_SQLSERVER_CITY, BRONZE_SQLSERVER_CUSTOMER, BRONZE_SQLSERVER_STATE, DIM_CUSTOMER,
)
from banking_dwh.dq.checks import DataQualityError
from banking_dwh.silver import dim_customer as dc

ROOT = Path(__file__).resolve().parents[1]
TALEND_ZIP = ROOT.parent / "talend_jobs" / "Load_DimCustomer.zip"
T0 = datetime(2026, 1, 1)
T1 = datetime(2026, 1, 2)


@pytest.fixture(scope="module")
def cfg(spark):
    c = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t7_")
    for layer in ("bronze", "silver", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c.schema(layer)}")
    return c


@pytest.fixture()
def clean(spark, cfg):
    for t in (DIM_CUSTOMER, BRONZE_SQLSERVER_CUSTOMER, BRONZE_SQLSERVER_CITY, BRONZE_SQLSERVER_STATE):
        spark.sql(f"DROP TABLE IF EXISTS {cfg.table(t.layer, t.name)}")
    spark.sql(f"DROP TABLE IF EXISTS {cfg.table('ops', 'dq_results')}")
    return cfg


def _cust(cid, name, addr, city_id, age, gender, email, at=T0, batch="b1"):
    return (cid, name, addr, city_id, age, gender, email, at, batch)


CUSTOMERS = [
    _cust(1, "Shelly Juwita", "Jl. Boulevard No. 31", 2, "25", "female", "Shelly@Gmail.com"),
    _cust(2, "Bobi Rinaldo", "Jl. Mangga No. 1", 999, "abc", "male", "bobi@gmail.com"),   # unknown city
    _cust(3, None, None, None, None, None, None),                                          # all nulls
    _cust(4, "Adam Malik", "  Jl. Kincir  ", 7, "4x", "Male ", "Adam@gmail.com"),         # city w/o state
    _cust(5, "Ria Addini", "Jl. Arjuna", 2, "", "female", "ria@x.id"),
]
CITIES = [(2, "Kelapa Gading", 1, T0, "b1"), (7, "Cempaka Putih", 99, T0, "b1")]
STATES = [(1, "Jakarta Utara", T0, "b1")]


def _df(spark, rows, contract):
    return spark.createDataFrame(rows, contract.ddl_schema())


def _write(spark, cfg, rows, contract, mode="overwrite"):
    spark.sql(dc.create_table_sql(cfg, contract, unity_catalog=False))
    _df(spark, rows, contract).write.format("delta").mode(mode).saveAsTable(cfg.table("bronze", contract.name))


def _seed(spark, cfg, customers=CUSTOMERS, cities=CITIES, states=STATES):
    _write(spark, cfg, customers, BRONZE_SQLSERVER_CUSTOMER)
    _write(spark, cfg, cities, BRONZE_SQLSERVER_CITY)
    _write(spark, cfg, states, BRONZE_SQLSERVER_STATE)


def _transform(spark, customers=CUSTOMERS, cities=CITIES, states=STATES):
    out = dc.transform(
        _df(spark, customers, BRONZE_SQLSERVER_CUSTOMER),
        _df(spark, cities, BRONZE_SQLSERVER_CITY),
        _df(spark, states, BRONZE_SQLSERVER_STATE),
    )
    return {r.customer_id: r for r in out.collect()}, out


def _silver(spark, cfg):
    return {r.customer_id: r for r in spark.table(cfg.table("silver", "dim_customer")).collect()}


# ------------------------------------------------------------------ Talend XML facts
def _talend_item() -> str:
    with zipfile.ZipFile(TALEND_ZIP) as z:
        name = next(n for n in z.namelist() if n.endswith("Load_DimCustomer_0.1.item"))
        return z.read(name).decode("utf-8")


def test_talend_xml_left_joins_upcase_and_no_trim():
    xml = _talend_item()
    assert "innerJoin" not in xml  # both tMap lookups are LEFT OUTER
    upcased = set(re.findall(r'expression="StringHandling\.UPCASE\(row1\.(\w+)\)', xml))
    assert upcased == set(dc.UPPERCASED_COLUMNS)
    assert xml.count("StringHandling.") == 3
    assert set(re.findall(r'TRIM_ALL_COLUMN" value="(\w+)"', xml)) == {"false"}
    assert 'elementRef="TRIM" value="true"' not in xml
    assert 'expression="row2.state_id "' in xml and 'expression="row1.city_id "' in xml


# ------------------------------------------------------------------ transform
def test_output_matches_contract_schema(spark):
    _, df = _transform(spark)
    assert df.columns == DIM_CUSTOMER.column_names
    types = {f.name: f.dataType.simpleString() for f in df.schema}
    assert types["age"] == "int" and types["customer_id"] == "int" and types["_loaded_at"] == "timestamp"


def test_left_joins_keep_customers_with_missing_city_or_state(spark):
    rows, _ = _transform(spark)
    assert sorted(rows) == [1, 2, 3, 4, 5]
    assert (rows[1].city_name, rows[1].state_name) == ("Kelapa Gading", "Jakarta Utara")
    assert (rows[2].city_name, rows[2].state_name) == (None, None)       # unknown city_id
    assert (rows[3].city_name, rows[3].state_name) == (None, None)       # null city_id
    assert (rows[4].city_name, rows[4].state_name) == ("Cempaka Putih", None)  # city's state missing


def test_uppercase_only_name_address_gender_and_no_trim(spark):
    rows, _ = _transform(spark)
    r = rows[1]
    assert (r.customer_name, r.address, r.gender) == ("SHELLY JUWITA", "JL. BOULEVARD NO. 31", "FEMALE")
    assert r.email == "Shelly@Gmail.com" and r.city_name == "Kelapa Gading" and r.state_name == "Jakarta Utara"
    assert (rows[4].address, rows[4].gender) == ("  JL. KINCIR  ", "MALE ")  # no trimming
    assert (rows[3].customer_name, rows[3].address, rows[3].gender, rows[3].email) == (None,) * 4


def test_age_cast_to_int_non_numeric_is_null(spark):
    rows, _ = _transform(spark)
    assert rows[1].age == 25
    assert rows[2].age is None  # 'abc'
    assert rows[3].age is None  # NULL
    assert rows[4].age is None  # '4x'
    assert rows[5].age is None  # ''


def test_source_is_deduped_on_key_latest_ingest_wins(spark):
    customers = CUSTOMERS + [_cust(1, "Shelly New", "Jl. Baru", 2, "26", "female", "s@x.id", at=T1, batch="b2")]
    cities = CITIES + [(2, "Kelapa Gading Barat", 1, T1, "b2")]
    rows, df = _transform(spark, customers, cities)
    assert df.count() == 5
    assert (rows[1].customer_name, rows[1].age, rows[1].city_name) == ("SHELLY NEW", 26, "Kelapa Gading Barat")
    assert rows[5].city_name == "Kelapa Gading Barat"


def test_merge_sql_is_null_safe_and_uses_cfg_names(cfg):
    sql = dc.merge_sql(cfg.table("silver", "dim_customer"), "src_view")
    assert sql.startswith("MERGE INTO spark_catalog.t7_silver.dim_customer AS t")
    assert "ON t.customer_id = s.customer_id" in sql
    assert "NOT (t.age <=> s.age)" in sql and "_loaded_at <=>" not in sql
    with pytest.raises(ValueError):
        dc.merge_sql("x", "bad-view; DROP")


def test_no_column_try_cast_for_serverless():
    src = (ROOT / "src" / "banking_dwh" / "silver" / "dim_customer.py").read_text()
    assert ".try_cast(" not in src


# ------------------------------------------------------------------ load / MERGE
def test_load_merges_and_is_idempotent(spark, clean):
    cfg = clean
    _seed(spark, cfg)
    m1 = dc.load(spark, cfg, unity_catalog=False)
    assert m1["target_rows"] == 5 and m1["merge"]["numTargetRowsInserted"] == 5
    assert m1["customers_without_city"] == 2 and m1["customers_without_state"] == 3
    first = _silver(spark, cfg)

    m2 = dc.load(spark, cfg, unity_catalog=False)
    assert m2["target_rows"] == 5
    assert m2["merge"].get("numTargetRowsInserted", 0) == 0 and m2["merge"].get("numTargetRowsUpdated", 0) == 0
    second = _silver(spark, cfg)
    assert first == second  # including _loaded_at: unchanged rows are not rewritten


def test_rerun_applies_source_changes_and_new_rows(spark, clean):
    cfg = clean
    _seed(spark, cfg)
    dc.load(spark, cfg, unity_catalog=False)
    before = _silver(spark, cfg)
    _write(spark, cfg, [
        _cust(2, "Bobi Rinaldo", "Jl. Mangga No. 1", 2, "31", "male", "bobi@gmail.com", at=T1, batch="b2"),
        _cust(6, "Rio Wijaya", "Jl. Abdul Muis", 2, "52", "male", "rio@x.id", at=T1, batch="b2"),
    ], BRONZE_SQLSERVER_CUSTOMER, mode="append")
    m = dc.load(spark, cfg, unity_catalog=False)
    assert (m["merge"]["numTargetRowsInserted"], m["merge"]["numTargetRowsUpdated"]) == (1, 1)
    after = _silver(spark, cfg)
    assert len(after) == 6
    assert (after[2].city_name, after[2].age) == ("Kelapa Gading", 31)
    assert after[2]._loaded_at >= before[2]._loaded_at
    assert after[1] == before[1]


def test_dq_pk_checks_pass_and_are_recorded(spark, clean):
    cfg = clean
    _seed(spark, cfg)
    dc.load(spark, cfg, unity_catalog=False)
    res = dc.run_dq(spark, cfg, "t7-run")
    assert {r["check_name"] for r in res} == {"pk_not_null.dim_customer", "pk_unique.dim_customer"}
    assert all(r["passed"] for r in res)
    assert spark.table(cfg.table("ops", "dq_results")).filter("run_id = 't7-run'").count() == 2


def test_dq_fails_on_duplicate_key(spark, clean):
    cfg = clean
    _seed(spark, cfg)
    dc.load(spark, cfg, unity_catalog=False)
    tbl = cfg.table("silver", "dim_customer")
    spark.table(tbl).filter("customer_id = 1").write.format("delta").mode("append").saveAsTable(tbl)
    with pytest.raises(DataQualityError, match="pk_unique.dim_customer"):
        dc.run_dq(spark, cfg, "t7-dup")
    res = dc.run_dq(spark, cfg, "t7-dup2", fail_on_error=False)
    assert {r["check_name"]: r["failed_rows"] for r in res}["pk_unique.dim_customer"] == 1


# ------------------------------------------------------------------ sample.bak parity
def _expected():
    with open(dc.FIXTURE_DIR / "expected_dim_customer.csv", newline="", encoding="utf-8") as fh:
        return {int(r["customer_id"]): r for r in csv.DictReader(fh)}


def test_sample_bak_fixture_counts():
    assert len(dc.read_fixture("customer", BRONZE_SQLSERVER_CUSTOMER)) == 20
    assert len(dc.read_fixture("city", BRONZE_SQLSERVER_CITY)) == 52
    assert len(dc.read_fixture("state", BRONZE_SQLSERVER_STATE)) == 9


def test_parity_with_legacy_dimcustomer_from_sample_bak(spark, clean):
    cfg = clean
    seeded = dc.seed_bronze_from_fixture(spark, cfg, unity_catalog=False)
    assert seeded == {"sqlserver_customer": 20, "sqlserver_city": 52, "sqlserver_state": 9}
    m = dc.load(spark, cfg, unity_catalog=False)
    assert m["target_rows"] == 20 and m["customers_without_city"] == 0
    got = _silver(spark, cfg)
    expected = _expected()
    assert sorted(got) == sorted(expected)
    for cid, e in expected.items():
        g = got[cid]
        assert (g.customer_name, g.address, g.city_name, g.state_name, g.age, g.gender, g.email) == (
            e["customer_name"], e["address"], e["city_name"], e["state_name"], int(e["age"]), e["gender"], e["email"]
        ), cid
    assert all(r["passed"] for r in dc.run_dq(spark, cfg, "t7-parity"))
    p = dc.parity_with_fixture(spark, cfg)
    assert p["match"] and (p["expected_rows"], p["actual_rows"]) == (20, 20)


def test_parity_helper_reports_mismatches(spark, clean):
    cfg = clean
    dc.seed_bronze_from_fixture(spark, cfg, unity_catalog=False)
    dc.load(spark, cfg, unity_catalog=False)
    tbl = cfg.table("silver", "dim_customer")
    spark.sql(f"UPDATE {tbl} SET gender = 'female' WHERE customer_id = 1")
    spark.sql(f"DELETE FROM {tbl} WHERE customer_id = 20")
    p = dc.parity_with_fixture(spark, cfg)
    assert (p["match"], p["mismatched"], p["missing"]) == (False, [1], [20])


def test_parity_helper_flags_duplicate_rows(spark, clean):
    cfg = clean
    dc.seed_bronze_from_fixture(spark, cfg, unity_catalog=False)
    dc.load(spark, cfg, unity_catalog=False)
    tbl = cfg.table("silver", "dim_customer")
    spark.sql(f"INSERT INTO {tbl} SELECT * FROM {tbl} WHERE customer_id = 1")
    p = dc.parity_with_fixture(spark, cfg)
    assert (p["match"], p["actual_rows"], p["duplicate_keys"], p["mismatched"]) == (False, 21, [1], [])


def test_dedupe_tie_break_is_deterministic(spark):
    from datetime import datetime

    ts = datetime(2026, 1, 1)
    rows = [(1, "b", ts, "x"), (1, "a", ts, "x"), (2, "c", ts, "x")]
    df = spark.createDataFrame(rows, "id INT, v STRING, _ingested_at TIMESTAMP, _batch_id STRING")
    for d in (df, df.orderBy("v"), df.repartition(3)):
        assert sorted((r.id, r.v) for r in dc.latest_per_key(d, "id").collect()) == [(1, "a"), (2, "c")]


def test_seed_refuses_shared_namespace_and_prod(spark):
    shared = EnvConfig.from_target("dev", catalog="spark_catalog")  # bdwh_ = shared dev prefix
    with pytest.raises(ValueError, match="shared namespace"):
        dc.seed_bronze_from_fixture(spark, shared, unity_catalog=False)
    with pytest.raises(ValueError, match="prod"):
        dc.seed_bronze_from_fixture(spark, EnvConfig.from_target("prod", schema_prefix="x_"), unity_catalog=False)
    # dev label but resolving into the prod catalog (schema_prefix="-" -> "") or the dev shared namespace
    for bad in (
        EnvConfig.from_target("dev", catalog="banking_dwh", schema_prefix=""),
        EnvConfig.from_target("dev", catalog="banking_dwh", schema_prefix="scratch_"),
        EnvConfig.from_target("dev", schema_prefix="bdwh_"),
        EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix=""),
    ):
        with pytest.raises(ValueError, match="Refusing"):
            dc.seed_bronze_from_fixture(spark, bad, unity_catalog=False)
    dc.assert_scratch_namespace(EnvConfig.from_target("dev", schema_prefix="bdwh_t7_"))
