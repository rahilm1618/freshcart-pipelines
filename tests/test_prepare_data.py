"""
tests/test_prepare_data.py
---------------------------
Part A3 – CI pytest test for the prepare_data transformer block.

The block can be imported outside Mage because it only imports its
decorators conditionally (when the mage_ai package is present).
"""

from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform


def test_split_is_80_20():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30


def test_split_column_exists():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert "split" in out.columns


def test_no_rows_lost():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert len(out) == len(df) == 150
