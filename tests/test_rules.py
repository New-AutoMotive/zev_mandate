import textwrap

import pytest

from zevm import rules


def write(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(textwrap.dedent(body))
    return p


@pytest.fixture
def base(tmp_path):
    return write(tmp_path, "base.yaml", """
        name: base
        vehicle_types:
          car:
            zev_target: {2024: 0.22, 2025: 0.28, 2030: 0.80}
            borrowing:
              cap: {2024: 0.75, 2027: 0.0}
              repay_multiplier: 1.03
    """)


def test_year_table_carries_forward(base):
    p = rules.load(base)
    assert p.zev_target("car", 2024) == 0.22
    assert p.zev_target("car", 2027) == 0.28  # not listed: carries 2025 forward
    assert p.zev_target("car", 2035) == 0.80


def test_year_before_table_start_raises(base):
    with pytest.raises(KeyError):
        rules.load(base).zev_target("car", 2023)


def test_scalar_values_ignore_year(base):
    assert rules.load(base).get("car", "borrowing.repay_multiplier", 2029) == 1.03


def test_extends_overrides_single_year_only(tmp_path, base):
    child = write(tmp_path, "child.yaml", """
        extends: base.yaml
        name: slower 2026
        vehicle_types:
          car:
            zev_target: {2026: 0.30}
            borrowing:
              repay_multiplier: 1.0
    """)
    p = rules.load(child)
    assert p.name == "slower 2026"
    assert p.zev_target("car", 2025) == 0.28
    assert p.zev_target("car", 2026) == 0.30
    assert p.zev_target("car", 2030) == 0.80
    assert p.get("car", "borrowing.cap", 2024) == 0.75  # untouched sibling kept
    assert p.get("car", "borrowing.repay_multiplier") == 1.0


def test_circular_extends_rejected(tmp_path):
    write(tmp_path, "a.yaml", "extends: b.yaml\n")
    write(tmp_path, "b.yaml", "extends: a.yaml\n")
    with pytest.raises(ValueError, match="Circular"):
        rules.load(tmp_path / "a.yaml")


def test_target_given_as_percent_rejected(tmp_path):
    p = write(tmp_path, "bad.yaml", """
        vehicle_types:
          car:
            zev_target: {2024: 22}
    """)
    with pytest.raises(ValueError, match="between 0 and 1"):
        rules.load(p)
