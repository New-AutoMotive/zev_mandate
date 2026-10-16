"""Offline tests for zevm.data: no BigQuery access needed."""
import datetime as dt
import json

import pandas as pd
import pytest

from zevm import data, rules


def raw_rows(rows):
    cols = ["month", "region", "make", "manufacturer", "fuel_type", "registrations", "with_co2", "co2_sum"]
    return pd.DataFrame(rows, columns=cols)


POOLS = pd.DataFrame({
    "make": ["AUDI", "SKODA", "TESLA"],
    "regulated_entity": ["AUDI AG", "SKODA AUTO AS", "TESLA INC"],
    "compliance_pool": ["VOLKSWAGEN PC (POOL)", "VOLKSWAGEN PC (POOL)", "Standalone"],
    "co2_baseline": [139.011, 139.011, None],
})


def test_classify_zev_and_co2_imputation():
    df = data.classify(raw_rows([
        ["2025-01-01", "GB", "AUDI", "AUDI AG", "ELECTRICITY", 10, 1, 50.0],     # stray CO2 on a BEV ignored
        ["2025-01-01", "GB", "AUDI", "AUDI AG", "DIESEL", 10, 8, 1200.0],        # 2 missing -> filled at 150
        ["2025-01-01", "GB", "AUDI", "AUDI AG", "FUEL CELLS", 3, 1, 200.0],      # 2 zero-CO2 ZEVs, 1 non-ZEV
        ["2025-01-01", "GB", "SKODA", "SKODA AUTO AS", "PETROL", 4, 0, 0.0],     # no CO2 in row -> market mean
        ["2025-01-01", "GB", "AUDI", "AUDI AG", "PETROL", 2, 2, 260.0],          # petrol market mean = 130
    ]))
    assert df.zev_registrations.tolist() == [10, 0, 2, 0, 0]
    assert df.nonzev_registrations.tolist() == [0, 10, 1, 4, 2]
    assert df.co2_activity.tolist() == pytest.approx([0, 1500, 200, 520, 260])
    assert df.nonzev_without_co2.sum() == 0


def test_classify_flags_rows_with_nothing_to_fill_from():
    df = data.classify(raw_rows([["2025-01-01", "GB", "X", "X", "GAS", 5, 0, 0.0]]))
    assert df.co2_activity.iloc[0] == 0
    assert df.nonzev_without_co2.iloc[0] == 5


def test_summarise_by_pool_merges_brands_and_keeps_standalone_entities():
    raw = raw_rows([
        ["2025-01-01", "GB", "AUDI", "AUDI AG", "ELECTRICITY", 30, 0, 0.0],
        ["2025-01-01", "GB", "SKODA", "SKODA AUTO AS", "PETROL", 70, 70, 9100.0],
        ["2025-01-01", "GB", "TESLA", "TESLA INC", "ELECTRICITY", 50, 0, 0.0],
        ["2025-01-01", "GB", "NEWCO", "NEWCO LTD", "PETROL", 5, 5, 600.0],      # not in pools table
    ])
    out = data.summarise(raw, by="pool", pools=POOLS).set_index("group")
    assert out.loc["VOLKSWAGEN PC (POOL)", "total_registrations"] == 100
    assert out.loc["VOLKSWAGEN PC (POOL)", "zev_share"] == pytest.approx(0.30)
    assert out.loc["VOLKSWAGEN PC (POOL)", "mean_nonzev_co2"] == pytest.approx(130)
    assert out.loc["TESLA INC", "zev_registrations"] == 50
    assert "NEWCO LTD" in out.index


def test_summarise_drops_ni_only_in_gb_only_years():
    raw = raw_rows([
        ["2024-06-01", "GB", "AUDI", "AUDI AG", "PETROL", 10, 10, 1000.0],
        ["2024-06-01", "NI", "AUDI", "AUDI AG", "PETROL", 1, 1, 100.0],
        ["2025-06-01", "GB", "AUDI", "AUDI AG", "PETROL", 10, 10, 1000.0],
        ["2025-06-01", "NI", "AUDI", "AUDI AG", "PETROL", 1, 1, 100.0],
        ["2024-06-01", "PER", "AUDI", "AUDI AG", "PETROL", 2, 2, 200.0],
    ])
    out = data.summarise(raw, by="entity", freq="year", params=rules.load("vets")).set_index("year")
    assert out.loc[2024, "total_registrations"] == 12   # GB + PER, NI dropped
    assert out.loc[2025, "total_registrations"] == 11   # whole UK


def test_pool_grouping_refused_for_vans():
    with pytest.raises(ValueError, match="cars only"):
        data.summarise(raw_rows([]), by="pool", vehicle_type="van")


def test_last_complete_month_end():
    assert data.last_complete_month_end(dt.date(2026, 1, 15)) == dt.date(2026, 1, 1)


class FakeJob:
    total_bytes_processed = 123

    def __init__(self, df):
        self.df = df

    def to_dataframe(self, **_):
        return self.df


class FakeClient:
    def __init__(self, df):
        self.df, self.calls = df, 0

    def query(self, sql, job_config=None):
        self.calls += 1
        assert "{project}" not in sql
        return FakeJob(self.df)


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(data, "CACHE_DIR", tmp_path)
    return tmp_path


def test_query_caches_and_reuses(cache):
    client = FakeClient(pd.DataFrame({"make": ["AUDI"], "n": [1]}))
    a = data.query("pool_baselines", client=client)
    b = data.query("pool_baselines", client=client)
    assert client.calls == 1
    pd.testing.assert_frame_equal(a, b)
    meta = json.loads(next(cache.glob("*.json")).read_text())
    assert meta["rows"] == 1 and meta["bytes_processed"] == 123


def test_query_new_params_or_refresh_pulls_again(cache):
    client = FakeClient(pd.DataFrame({"n": [1]}))
    p = {"start": dt.date(2025, 1, 1), "end": dt.date(2026, 1, 1)}
    data.query("reported_monthly", p, client=client)
    data.query("reported_monthly", {**p, "end": dt.date(2026, 2, 1)}, client=client)
    data.query("reported_monthly", p, client=client, refresh=True)
    assert client.calls == 3


def test_query_stale_cache_pulls_again_but_offline_uses_it(cache):
    client = FakeClient(pd.DataFrame({"n": [1]}))
    data.query("pool_baselines", client=client)
    meta_path = next(cache.glob("*.json"))
    meta = json.loads(meta_path.read_text())
    meta["pulled_at"] = "2020-01-01T00:00:00+00:00"
    meta_path.write_text(json.dumps(meta))
    data.query("pool_baselines", client=client, offline=True)
    assert client.calls == 1
    data.query("pool_baselines", client=client)
    assert client.calls == 2


def test_offline_without_cache_raises(cache):
    with pytest.raises(FileNotFoundError):
        data.query("pool_baselines", offline=True)


def test_dbdate_columns_survive_parquet(cache):
    import db_dtypes  # noqa: F401  registers the dbdate dtype, as BigQuery returns it
    df = pd.DataFrame({"month": pd.Series([dt.date(2025, 1, 1)], dtype="dbdate"), "n": pd.array([1], dtype="Int64")})
    data.query("pool_baselines", client=FakeClient(df))
    path = next(cache.glob("*.parquet"))
    import subprocess, sys
    # Read back in a fresh interpreter without db_dtypes imported.
    code = f"import pandas as pd; d = pd.read_parquet({str(path)!r}); print(d.month.dtype, d.n.dtype)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
    month_dtype, n_dtype = out.split()
    assert month_dtype.startswith("datetime64") and n_dtype == "int64"
