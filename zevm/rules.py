"""Load mandate parameters from YAML.

A params file describes one version of the policy. A file can `extends:` another
(path relative to itself) and override only the values that differ, so a scenario
like "no borrowing after 2026" is a few lines on top of the legislated base.

Year-keyed tables are written as YAML mappings ({2024: 0.22, 2025: 0.28}). Years
missing from a table take the value of the latest earlier year, so a trajectory
only needs to list the years where it changes.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PARAMS_DIR = Path(__file__).resolve().parent.parent / "params"


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict) and not _is_year_table(value):
            out[key] = _deep_merge(out[key], value)
        elif isinstance(value, dict) and isinstance(out.get(key), dict):
            # Year tables merge year by year, so an override can change one year.
            out[key] = {**out[key], **value}
        else:
            out[key] = copy.deepcopy(value)
    return out


def _is_year_table(d: dict) -> bool:
    return bool(d) and all(isinstance(k, int) and 1990 < k < 2100 for k in d)


def _load_raw(path: Path, seen: tuple[Path, ...] = ()) -> dict:
    path = path.resolve()
    if path in seen:
        raise ValueError(f"Circular 'extends' chain: {' -> '.join(map(str, seen + (path,)))}")
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    parent = raw.pop("extends", None)
    if parent is None:
        return raw
    base = _load_raw((path.parent / parent), seen + (path,))
    return _deep_merge(base, raw)


def year_value(table: Any, year: int) -> Any:
    """Look up a year table, carrying the latest earlier value forward.

    Non-table values are returned unchanged, so a constant can be written as a scalar.
    """
    if not isinstance(table, dict) or not _is_year_table(table):
        return table
    earlier = [y for y in table if y <= year]
    if not earlier:
        raise KeyError(f"No value for {year}; table starts in {min(table)}")
    return table[max(earlier)]


@dataclass(frozen=True)
class Params:
    name: str
    data: dict
    path: Path

    def vehicle(self, vehicle_type: str) -> dict:
        try:
            return self.data["vehicle_types"][vehicle_type]
        except KeyError:
            raise KeyError(f"{self.name} has no rules for vehicle type {vehicle_type!r}") from None

    @property
    def scheme_years(self) -> range:
        first, last = self.data.get("scheme_years", [1900, 2100])
        return range(first, last + 1)

    def get(self, vehicle_type: str, dotted_key: str, year: int | None = None) -> Any:
        """e.g. params.get("car", "co2_to_zev.cap", 2026)."""
        if year is not None and year not in self.scheme_years:
            raise KeyError(
                f"{year} is outside {self.name} scheme years "
                f"{self.scheme_years.start}-{self.scheme_years.stop - 1}"
            )
        node: Any = self.vehicle(vehicle_type)
        for part in dotted_key.split("."):
            node = node[part]
        return year_value(node, year) if year is not None else node

    def zev_target(self, vehicle_type: str, year: int) -> float:
        return self.get(vehicle_type, "zev_target", year)


def load(name_or_path: str | Path) -> Params:
    """Load a params file by path, or by name from the params/ folder."""
    path = Path(name_or_path)
    if not path.suffix:
        path = PARAMS_DIR / f"{name_or_path}.yaml"
    data = _load_raw(path)
    _validate(data, path)
    return Params(name=data.get("name", path.stem), data=data, path=path)


def _validate(data: dict, path: Path) -> None:
    vts = data.get("vehicle_types")
    if not vts:
        raise ValueError(f"{path}: no vehicle_types defined")
    for vt, rules in vts.items():
        targets = rules.get("zev_target")
        if not isinstance(targets, dict) or not _is_year_table(targets):
            raise ValueError(f"{path}: {vt}.zev_target must be a year table")
        bad = {y: v for y, v in targets.items() if not 0 <= v <= 1}
        if bad:
            raise ValueError(f"{path}: {vt}.zev_target values must be shares between 0 and 1, got {bad}")
