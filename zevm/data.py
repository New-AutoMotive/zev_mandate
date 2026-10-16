"""Registrations data from BigQuery, cached locally as parquet.

    from zevm import data
    raw = data.registrations("car")                 # month x make x entity x fuel x region
    monthly = data.summarise(raw, by="pool")        # per compliance pool per month
    yearly = data.summarise(raw, by="pool", freq="year", params=rules.load("vets"))

Queries live in sql/*.sql. `{project}` is filled in from ZEVM_BQ_PROJECT (default
rugged-baton-283921) and dates are passed as query parameters. Each result is cached in
data/cache/ under a key built from the SQL text and the parameters, so editing a query or
moving the date window triggers a fresh pull automatically. A cached result older than
`max_age_days` is pulled again, because VES backfills late registrations.

Authentication uses Application Default Credentials:
    gcloud auth application-default login
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Literal

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"
CACHE_DIR = ROOT / "data" / "cache"
PROJECT = os.environ.get("ZEVM_BQ_PROJECT", "rugged-baton-283921")

VehicleType = Literal["car", "van"]

# VES fuelType values. FUEL CELLS rows count as ZEV only where CO2 is 0 (art. 3(7)).
# In the 2021-26 data every FUEL CELLS row has a CO2 value, so in practice they are non-ZEV.
ZEV_FUELS = {"ELECTRICITY"}
ZEV_IF_ZERO_CO2_FUELS = {"FUEL CELLS"}


# ---------------------------------------------------------------- query + cache

def _client():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def _read_sql(name: str) -> str:
    return (SQL_DIR / f"{name}.sql").read_text().replace("{project}", PROJECT)


def _cache_paths(name: str, sql: str, params: dict) -> tuple[Path, Path]:
    key = hashlib.sha1((sql + json.dumps(params, sort_keys=True, default=str)).encode()).hexdigest()[:12]
    stem = CACHE_DIR / f"{name}-{key}"
    return stem.with_suffix(".parquet"), stem.with_suffix(".json")


def query(
    name: str,
    params: dict | None = None,
    *,
    refresh: bool = False,
    offline: bool = False,
    max_age_days: float = 7,
    client=None,
) -> pd.DataFrame:
    """Run sql/<name>.sql with date `params`, using the local cache when fresh.

    offline=True never touches BigQuery: it returns the cached result whatever its age,
    or raises if there is none.
    """
    params = params or {}
    sql = _read_sql(name)
    data_path, meta_path = _cache_paths(name, sql, params)

    if data_path.exists() and not refresh:
        meta = json.loads(meta_path.read_text())
        age = dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(meta["pulled_at"])
        if offline or age < dt.timedelta(days=max_age_days):
            return pd.read_parquet(data_path)
    if offline:
        raise FileNotFoundError(f"No cached result for {name} with {params}; run once with BigQuery access")

    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, "DATE", v) for k, v in params.items()]
    )
    job = (client or _client()).query(sql, job_config=job_config)
    df = job.to_dataframe(create_bqstorage_client=False)
    df = _plain_dtypes(df)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(data_path, index=False)
    meta_path.write_text(json.dumps({
        "query": name,
        "params": {k: str(v) for k, v in params.items()},
        "pulled_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "rows": len(df),
        "bytes_processed": getattr(job, "total_bytes_processed", None),
    }, indent=2))
    return df


def _plain_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """BigQuery DATE comes back as db-dtypes 'dbdate', which parquet can't read back
    without db_dtypes installed. Convert to datetime64 and nullable ints to int64."""
    df = df.copy()
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype == "dbdate":
            df[col] = pd.to_datetime(df[col].astype(str))
        elif dtype == "Int64" and not df[col].isna().any():
            df[col] = df[col].astype("int64")
    return df


def last_complete_month_end(today: dt.date | None = None) -> dt.date:
    """Exclusive end date: the first day of the current month."""
    today = today or dt.date.today()
    return today.replace(day=1)


# ---------------------------------------------------------------- datasets

def registrations(
    vehicle_type: VehicleType = "car",
    start: str | dt.date = "2021-01-01",
    end: str | dt.date | None = None,
    **query_kwargs,
) -> pd.DataFrame:
    """Monthly registrations at month x region x make x manufacturer x fuel_type.

    `end` is exclusive and defaults to the start of the current month, so the partial
    current month is left out.
    """
    params = {"start": pd.Timestamp(start).date(), "end": pd.Timestamp(end or last_complete_month_end()).date()}
    return query(f"registrations_monthly_{vehicle_type}", params, **query_kwargs)


def pool_baselines(**query_kwargs) -> pd.DataFrame:
    """Official car pools and CO2 baselines, one row per make."""
    return query("pool_baselines", **query_kwargs)


def entity_baselines(vehicle_type: VehicleType = "car", **query_kwargs) -> pd.DataFrame:
    """Schedule 1 CO2 baselines estimated per legal entity."""
    return query(f"entity_baselines_{vehicle_type}", **query_kwargs)


def reported_monthly(start: str | dt.date = "2021-01-01", end: str | dt.date | None = None, **query_kwargs) -> pd.DataFrame:
    params = {"start": pd.Timestamp(start).date(), "end": pd.Timestamp(end or last_complete_month_end()).date()}
    return query("reported_monthly", params, **query_kwargs)


# ---------------------------------------------------------------- shaping

def classify(raw: pd.DataFrame) -> pd.DataFrame:
    """Split each row into ZEV / non-ZEV counts and non-ZEV CO2 activity (g/km summed).

    Non-ZEVs with no CO2 value get the mean of their make/fuel/month row, or failing that
    the market mean for that fuel and month, so missing values don't count as 0 g/km.
    """
    df = raw.copy()
    df["month"] = pd.to_datetime(df["month"])
    regs, with_co2, co2_sum = (df[c].astype(float) for c in ("registrations", "with_co2", "co2_sum"))

    is_zev_fuel = df["fuel_type"].isin(ZEV_FUELS)
    zero_co2_zev = df["fuel_type"].isin(ZEV_IF_ZERO_CO2_FUELS)
    df["zev_registrations"] = regs.where(is_zev_fuel, 0.0) + (regs - with_co2).where(zero_co2_zev, 0.0)
    df["nonzev_registrations"] = regs - df["zev_registrations"]

    # CO2 recorded against an ELECTRICITY row is a data error; ignore it.
    nz_with_co2 = with_co2.where(~is_zev_fuel, 0.0)
    nz_co2_sum = co2_sum.where(~is_zev_fuel, 0.0)
    # FUEL CELLS: the zero-CO2 ones are already counted as ZEV, so none are missing.
    missing = (df["nonzev_registrations"] - nz_with_co2).where(~zero_co2_zev, 0.0).clip(lower=0)

    row_mean = nz_co2_sum / nz_with_co2.where(nz_with_co2 > 0)
    market = (
        pd.DataFrame({"fuel_type": df["fuel_type"], "month": df["month"], "s": nz_co2_sum, "n": nz_with_co2})
        .groupby(["fuel_type", "month"])[["s", "n"]].transform("sum")
    )
    market_mean = market["s"] / market["n"].where(market["n"] > 0)
    fill = row_mean.fillna(market_mean)

    df["co2_imputed"] = (missing * fill).fillna(0.0)
    df["co2_activity"] = nz_co2_sum + df["co2_imputed"]
    df["nonzev_without_co2"] = missing.where(fill.isna(), 0.0)  # left at 0 g/km: no value to fill in from
    return df


def _group_key(df: pd.DataFrame, by: str, pools: pd.DataFrame | None) -> pd.Series:
    if by == "make":
        return df["make"]
    if by == "entity":
        return df["manufacturer"]
    if by == "pool":
        if pools is None:
            pools = pool_baselines()
        p = pools.drop_duplicates("make").set_index("make")
        pool = df["make"].map(p["compliance_pool"])
        entity = df["make"].map(p["regulated_entity"])
        key = pool.where(pool.ne("Standalone"), entity)
        # Makes not in the pools table fall back to their legal entity.
        return key.fillna(df["manufacturer"])
    raise ValueError(f"by must be 'make', 'entity' or 'pool', not {by!r}")


def summarise(
    raw: pd.DataFrame,
    by: Literal["make", "entity", "pool"] = "pool",
    freq: Literal["month", "year"] = "month",
    params=None,
    vehicle_type: VehicleType = "car",
    pools: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Totals per group per month (or scheme year) ready for the compliance engine.

    With `params` (a zevm.rules.Params), registrations are limited to the scheme's
    geography for each year: GB-only years drop Northern Ireland. Rows with region
    PER or unknown are kept.

    Pool grouping uses VETS_Pool_Baselines, which covers cars only.
    """
    if by == "pool" and vehicle_type != "car":
        raise ValueError("Pool mapping (VETS_Pool_Baselines) covers cars only; use by='entity' for vans")
    df = classify(raw)
    if params is not None:
        geo = df["month"].dt.year.map(lambda y: _geography(params, y))
        df = df[~((geo == "GB") & (df["region"] == "NI"))]

    df["group"] = _group_key(df, by, pools)
    period = df["month"].dt.year.rename("year") if freq == "year" else df["month"]
    cols = ["registrations", "zev_registrations", "nonzev_registrations", "co2_activity", "co2_imputed", "nonzev_without_co2"]
    out = df.groupby([df["group"], period])[cols].sum().reset_index()
    out = out.rename(columns={"registrations": "total_registrations"})
    out["zev_share"] = out["zev_registrations"] / out["total_registrations"]
    out["mean_nonzev_co2"] = out["co2_activity"] / out["nonzev_registrations"].where(out["nonzev_registrations"] > 0)
    return out


def _geography(params, year: int) -> str | None:
    from zevm.rules import year_value
    try:
        return year_value(params.data["geography"], year)
    except KeyError:
        return None
